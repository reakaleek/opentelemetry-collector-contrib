package githubactionslogreceiver

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/go-github/v60/github"
	"github.com/julienschmidt/httprouter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

type githubActionsLogReceiver struct {
	config      *Config
	consumer    consumer.Logs
	logger      *zap.Logger
	runLogCache runLogCache
	server      *http.Server
	wg          sync.WaitGroup
}

func newLogsReceiver(cfg *Config, logger *zap.Logger, consumer consumer.Logs) *githubActionsLogReceiver {
	return &githubActionsLogReceiver{
		config:      cfg,
		logger:      logger,
		runLogCache: rlc{},
		consumer:    consumer,
	}
}

func (ghalr *githubActionsLogReceiver) Start(_ context.Context, _ component.Host) error {
	endpoint := fmt.Sprintf("%s%s", ghalr.config.ServerConfig.Endpoint, ghalr.config.Path)
	ghalr.logger.Info("Starting the what receiver", zap.String("endpoint", endpoint))

	router := httprouter.New()
	router.POST(ghalr.config.Path, ghalr.handleWorkflowRun)
	router.GET(ghalr.config.HealthCheckPath, ghalr.handleHealthCheck)

	ghalr.server = &http.Server{
		Addr:    ghalr.config.ServerConfig.Endpoint,
		Handler: router,
	}

	ghalr.wg.Add(1)
	go func() {
		defer ghalr.wg.Done()
		if err := ghalr.server.ListenAndServe(); errors.Is(err, http.ErrServerClosed) {
			ghalr.logger.Error("Receiver server has been shutdown", zap.Error(err))
		}
	}()
	return nil
}

func (ghalr *githubActionsLogReceiver) Shutdown(_ context.Context) error {
	if ghalr.server == nil {
		return nil
	}
	err := ghalr.server.Close()
	ghalr.wg.Wait()
	return err
}

func (ghalr *githubActionsLogReceiver) handleHealthCheck(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}

func (ghalr *githubActionsLogReceiver) handleWorkflowRun(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var response GitHubWorkflowRunEvent
	err := json.NewDecoder(r.Body).Decode(&response)
	if err != nil {
		ghalr.logger.Error("Failed to decode request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ghalr.logger.Info("Received a request", zap.String("path", r.URL.Path))
	ghalr.logger.Debug("Request Payload", zap.Any("response", response))
	if response.Action != "completed" {
		ghalr.logger.Info("Skipping the request because it is not a completed workflow run")
		w.WriteHeader(http.StatusOK)
		return
	}
	ghalr.logger.Info("Received a completed workflow run", zap.String("workflow_run url:", *response.WorkflowRun.URL))
	ghClient := github.NewClient(nil).WithAuthToken(string(ghalr.config.GitHubToken))
	workflowJobs, _, err := ghClient.Actions.ListWorkflowJobs(r.Context(), response.Repository.GetOwner().GetLogin(), response.Repository.GetName(), response.WorkflowRun.GetID(), nil)
	if err != nil {
		ghalr.logger.Error("Failed to get workflow Jobs", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ghalr.logger.Info("Received Jobs", zap.Any("Jobs", workflowJobs))
	runLogZip, err := getRunLog(
		ghalr.runLogCache,
		ghalr.logger,
		r.Context(), ghClient,
		http.DefaultClient,
		response.Repository,
		response.WorkflowRun,
	)
	if err != nil {
		ghalr.logger.Error("Failed to get run log", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer runLogZip.Close()
	jobs := mapJobs(workflowJobs.Jobs)
	attachRunLog(&runLogZip.Reader, jobs)
	run := mapRun(&response.WorkflowRun)
	repository := mapRepository(&response.Repository)
	logs, err := toLogs(repository, run, jobs)
	if err != nil {
		ghalr.logger.Error("Failed to convert Jobs to logs", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	err = ghalr.consumer.ConsumeLogs(r.Context(), logs)
	if err != nil {
		ghalr.logger.Error("Failed to consume logs", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
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

func getRunLog(cache runLogCache, logger *zap.Logger, ctx context.Context, ghClient *github.Client, httpClient *http.Client, repository github.Repository, workflowRun github.WorkflowRun) (*zip.ReadCloser, error) {
	filename := fmt.Sprintf("run-log-%d-%d.zip", workflowRun.ID, workflowRun.GetRunStartedAt().Unix())
	fp := filepath.Join(os.TempDir(), "run-log-cache", filename)
	logger.Info("Checking if log exists in cache", zap.String("path", fp))
	if !cache.Exists(fp) {
		logURL, _, err := ghClient.Actions.GetWorkflowRunLogs(ctx, repository.GetOwner().GetLogin(), repository.GetName(), workflowRun.GetID(), 4)
		logger.Info("Received logs url", zap.Any("url", logURL))
		if err != nil {
			logger.Error("Failed to get logs url", zap.Error(err))
			return nil, err
		}
		response, err := fetchLog(httpClient, logURL.String())
		defer response.Close()
		data, err := io.ReadAll(response)
		respReader := bytes.NewReader(data)
		// Check if the response is a valid zip format
		_, err = zip.NewReader(respReader, respReader.Size())
		if err != nil {
			return nil, err
		}
		err = cache.Create(fp, respReader)
		if err != nil {
			return nil, err
		}
	}
	return cache.Open(fp)
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
				if re.MatchString(file.Name) {
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
