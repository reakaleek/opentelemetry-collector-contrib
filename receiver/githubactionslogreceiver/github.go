package githubactionslogreceiver

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v60/github"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func createGitHubClient(githubAuth GitHubAuth) (*github.Client, error) {
	if githubAuth.AppID != 0 {
		if githubAuth.PrivateKey != "" {
			var privateKey []byte
			privateKey, err := base64.StdEncoding.DecodeString(string(githubAuth.PrivateKey))
			if err != nil {
				privateKey = []byte(githubAuth.PrivateKey)
			}
			itr, err := ghinstallation.New(
				http.DefaultTransport,
				githubAuth.AppID,
				githubAuth.InstallationID,
				privateKey,
			)
			if err != nil {
				return &github.Client{}, err
			}
			return github.NewClient(&http.Client{Transport: itr}), nil
		} else {
			itr, err := ghinstallation.NewKeyFromFile(
				http.DefaultTransport,
				githubAuth.AppID,
				githubAuth.InstallationID,
				githubAuth.PrivateKeyPath,
			)
			if err != nil {
				return &github.Client{}, err
			}
			return github.NewClient(&http.Client{Transport: itr}), nil
		}
	} else {
		return github.NewClient(nil).WithAuthToken(string(githubAuth.Token)), nil
	}
}

type githubRateLimit struct {
	limit     int
	remaining int
	reset     time.Time
}

func getWorkflowJobs(
	ctx context.Context,
	event github.WorkflowRunEvent,
	ghClient *github.Client,
) ([]*github.WorkflowJob, githubRateLimit, error) {
	listWorkflowJobsOpts := &github.ListOptions{
		PerPage: 100,
	}
	rateLimit := githubRateLimit{}
	var allWorkflowJobs []*github.WorkflowJob
	for {
		workflowJobs, response, err := ghClient.Actions.ListWorkflowJobsAttempt(
			ctx,
			event.GetRepo().GetOwner().GetLogin(),
			event.GetRepo().GetName(),
			event.GetWorkflowRun().GetID(),
			int64(event.GetWorkflowRun().GetRunAttempt()),
			listWorkflowJobsOpts,
		)
		if err != nil {
			return nil, githubRateLimit{}, fmt.Errorf("failed to get workflow Jobs: %w", err)
		}
		allWorkflowJobs = append(allWorkflowJobs, workflowJobs.Jobs...)
		if response.NextPage == 0 {
			rateLimit = parseRateLimitHeader(response, rateLimit)
			break
		}
		listWorkflowJobsOpts.Page = response.NextPage
	}
	return allWorkflowJobs, rateLimit, nil
}

func parseRateLimitHeader(response *github.Response, rateLimit githubRateLimit) githubRateLimit {
	limit, _ := strconv.Atoi(response.Header.Get("X-RateLimit-Limit"))
	remaining, _ := strconv.Atoi(response.Header.Get("X-RateLimit-Remaining"))
	reset, _ := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64)
	rateLimit = githubRateLimit{
		limit:     limit,
		remaining: remaining,
		reset:     time.Unix(reset, 0),
	}
	return rateLimit
}

func fetchLog(httpClient *http.Client, logURL string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", logURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get logs: %s", resp.Status)
	}
	return resp.Body, nil
}

type deleteRunLogFunc func() error

func getRunLog(
	cache runLogCache,
	logger *zap.Logger,
	ctx context.Context,
	ghClient *github.Client,
	httpClient *http.Client,
	repository *github.Repository,
	workflowRun *github.WorkflowRun,
) (*zip.ReadCloser, deleteRunLogFunc, error) {
	filename := fmt.Sprintf("run-log-%d-%d.zip", workflowRun.ID, workflowRun.GetRunStartedAt().Unix())
	fp := filepath.Join(os.TempDir(), "run-log-cache", filename)
	if !cache.Exists(fp) {
		logURL, _, err := ghClient.Actions.GetWorkflowRunLogs(
			ctx,
			repository.GetOwner().GetLogin(),
			repository.GetName(),
			workflowRun.GetID(),
			4,
		)
		if err != nil {
			logger.Error("Failed to get logs download url", zap.Error(err))
			return nil, nil, err
		}
		response, err := fetchLog(httpClient, logURL.String())
		if err != nil {
			return nil, nil, err
		}
		defer response.Close()
		err = cache.Create(fp, response)
		if err != nil {
			return nil, nil, err
		}
	}
	zipFile, err := cache.Open(fp)
	deleteFunc := func() error {
		return os.Remove(fp)
	}
	return zipFile, deleteFunc, err
}

// This function takes a zip file of logs and a list of Jobs.
// Structure of zip file
//
//	zip/
//	├── jobname1/
//	│   ├── 1_stepname.txt
//	│   ├── 2_anotherstepname.txt
//	│   ├── 3_stepstepname.txt
//	│   └── 4_laststepname.txt
//	└── jobname2/
//	    ├── 1_stepname.txt
//	    └── 2_somestepname.txt
//
// It iterates through the list of Jobs and tries to find the matching
// log in the zip file. If the matching log is found it is attached
// to the job.
func attachRunLog(rlz *zip.Reader, jobs []Job) {
	for i, job := range jobs {
		for j, step := range job.Steps {
			re := logFilenameRegexp(job, step)
			for _, file := range rlz.File {
				fileName := file.Name
				if re.MatchString(fileName) {
					jobs[i].Steps[j].Log = file
					break
				}
			}
		}
	}
}

// Copied from https://github.com/cli/cli/blob/b54f7a3bde50df3c31fdd68b638a0c0378a0ad58/pkg/cmd/run/view/view.go#L493
// to handle the case where the job or step name in the zip file is different from the job or step name in the object
func logFilenameRegexp(job Job, step Step) *regexp.Regexp {
	// As described in https://github.com/cli/cli/issues/5011#issuecomment-1570713070, there are a number of steps
	// the server can take when producing the downloaded zip file that can result in a mismatch between the job name
	// and the filename in the zip including:
	//  * Removing characters in the job name that aren't allowed in file paths
	//  * Truncating names that are too long for zip files
	//  * Adding collision deduplicating numbers for Jobs with the same name
	//
	// We are hesitant to duplicate all the server logic due to the fragility but while we explore our options, it
	// is sensible to fix the issue that is unavoidable for users, that when a job uses a composite action, the server
	// constructs a job name by constructing a job name of `<JOB_NAME`> / <ACTION_NAME>`. This means that logs will
	// never be found for Jobs that use composite actions.
	sanitizedJobName := strings.ReplaceAll(job.Name, "/", "")
	re := fmt.Sprintf(`%s\/%d_.*\.txt`, regexp.QuoteMeta(sanitizedJobName), step.Number)
	return regexp.MustCompile(re)
}
