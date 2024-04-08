package githubactionslogreceiver

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v60/github"
	"github.com/julienschmidt/httprouter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type githubActionsLogReceiver struct {
	config      *Config
	consumer    consumer.Logs
	logger      *zap.Logger
	runLogCache runLogCache
	server      *http.Server
	settings    receiver.CreateSettings
	ghClient    *github.Client
}

func newLogsReceiver(cfg *Config, params receiver.CreateSettings, consumer consumer.Logs) *githubActionsLogReceiver {
	return &githubActionsLogReceiver{
		config:      cfg,
		logger:      params.Logger,
		runLogCache: rlc{},
		consumer:    consumer,
		settings:    params,
	}
}

func (ghalr *githubActionsLogReceiver) Start(_ context.Context, host component.Host) error {
	var err error
	ghalr.ghClient, err = createGitHubClient(ghalr.config.GitHubAuth)
	if err != nil {
		return fmt.Errorf("failed to create GitHub client: %w", err)
	}
	endpoint := fmt.Sprintf("%s%s", ghalr.config.ServerConfig.Endpoint, ghalr.config.Path)
	ghalr.logger.Info("Starting receiver", zap.String("endpoint", endpoint))
	listener, err := ghalr.config.ServerConfig.ToListener()
	if err != nil {
		return err
	}
	router := httprouter.New()
	router.POST(ghalr.config.Path, ghalr.handleWorkflowRun)
	router.GET(ghalr.config.HealthCheckPath, ghalr.handleHealthCheck)
	ghalr.server, err = ghalr.config.ServerConfig.ToServer(host, ghalr.settings.TelemetrySettings, router)
	if err != nil {
		return err
	}
	go func() {
		if err := ghalr.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			ghalr.logger.Error("Receiver server has been shutdown", zap.Error(err))
			ghalr.settings.TelemetrySettings.ReportStatus(component.NewFatalErrorEvent(err))
		}
	}()
	return nil
}

func (ghalr *githubActionsLogReceiver) Shutdown(ctx context.Context) error {
	if ghalr.server == nil {
		return nil
	}
	return ghalr.server.Shutdown(ctx)
}

func (ghalr *githubActionsLogReceiver) handleHealthCheck(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}

func (ghalr *githubActionsLogReceiver) handleWorkflowRun(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var payload []byte
	var err error
	if ghalr.config.WebhookSecret == "" {
		payload, err = io.ReadAll(r.Body)
	} else {
		payload, err = github.ValidatePayload(r, []byte(string(ghalr.config.WebhookSecret)))
		if err != nil {
			ghalr.logger.Error("Invalid payload", zap.Error(err))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}
	event, err := github.ParseWebHook(github.WebHookType(r), payload)
	if err != nil {
		ghalr.logger.Error("Unable to parse payload", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch event := event.(type) {
	case *github.WorkflowRunEvent:
		processWorkflowRunEvent(ghalr, w, r, *event)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func processWorkflowRunEvent(
	ghalr *githubActionsLogReceiver,
	w http.ResponseWriter,
	r *http.Request,
	event github.WorkflowRunEvent,
) {
	if event.GetAction() != "completed" {
		ghalr.logger.Debug("Skipping the request because it is not a completed workflow run")
		w.WriteHeader(http.StatusOK)
		return
	}
	ghalr.logger.Info("Received Workflow Run Event", zap.String("github.workflow_run.url", event.GetWorkflowRun().GetHTMLURL()))
	ghalr.logger.Debug("Workflow Run Event", zap.Any("event", event))
	defer func() {
		rateLimits, _, err := ghalr.ghClient.RateLimit.Get(r.Context())
		if err != nil {
			ghalr.logger.Warn("Failed to get rate limits", zap.Error(err))
		}
		ghalr.logger.Info(
			"GitHub Api Rate limits",
			zap.Int("github.api.rate-limit.core.limit", rateLimits.Core.Limit),
			zap.Int("github.api.rate-limit.core.remaining", rateLimits.Core.Remaining),
			zap.Time("github.api.rate-limit.core.reset", rateLimits.Core.Reset.Time),
		)
	}()
	workflowJobs, _, err := ghalr.ghClient.Actions.ListWorkflowJobs(
		r.Context(),
		event.GetRepo().GetOwner().GetLogin(),
		event.GetRepo().GetName(),
		event.GetWorkflowRun().GetID(),
		nil,
	)
	if err != nil {
		ghalr.logger.Error("Failed to get workflow Jobs", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	runLogZip, deleteFunc, err := getRunLog(
		ghalr.runLogCache,
		ghalr.logger,
		r.Context(), ghalr.ghClient,
		http.DefaultClient,
		event.GetRepo(),
		event.GetWorkflowRun(),
	)
	if err != nil {
		ghalr.logger.Error("Failed to get run log", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := runLogZip.Close(); err != nil {
			ghalr.logger.Warn("Failed to close run log zip", zap.Error(err))
		}
		if err := deleteFunc(); err != nil {
			ghalr.logger.Warn("Failed to delete run log zip", zap.Error(err))
		}
	}()
	jobs := mapJobs(workflowJobs.Jobs)
	attachRunLog(&runLogZip.Reader, jobs)
	run := mapRun(event.GetWorkflowRun())
	repository := mapRepository(event.GetRepo())
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

func getRunLog(
	cache runLogCache,
	logger *zap.Logger,
	ctx context.Context,
	ghClient *github.Client,
	httpClient *http.Client,
	repository *github.Repository,
	workflowRun *github.WorkflowRun,
) (*zip.ReadCloser, func() error, error) {
	filename := fmt.Sprintf("run-log-%d-%d.zip", workflowRun.ID, workflowRun.GetRunStartedAt().Unix())
	fp := filepath.Join(os.TempDir(), "run-log-cache", filename)
	logger.Info("Checking if log exists in cache", zap.String("rlc.path", fp))
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
		defer response.Close()
		if err != nil {
			return nil, nil, err
		}
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
