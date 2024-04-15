package githubactionslogreceiver

import (
	"archive/zip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/google/go-github/v60/github"
	"github.com/julienschmidt/httprouter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type githubActionsLogReceiver struct {
	config      *Config
	consumer    consumer.Logs
	logger      *zap.Logger
	runLogCache runLogCache
	server      *http.Server
	settings    receiver.CreateSettings
	ghClient    *github.Client
	eventQueue  chan *github.WorkflowRunEvent
	wg          sync.WaitGroup
	wg2         sync.WaitGroup
}

func newLogsReceiver(cfg *Config, params receiver.CreateSettings, consumer consumer.Logs) *githubActionsLogReceiver {
	ghalr := &githubActionsLogReceiver{
		config:      cfg,
		logger:      params.Logger,
		runLogCache: rlc{},
		consumer:    consumer,
		settings:    params,
		eventQueue:  make(chan *github.WorkflowRunEvent, 1),
	}
	go ghalr.processEvents()
	return ghalr
}

func (ghalr *githubActionsLogReceiver) Start(_ context.Context, host component.Host) error {
	var err error
	ghalr.ghClient, err = createGitHubClient(ghalr.config.GitHubAuth)
	if err != nil {
		return err
	}
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
	router.POST(ghalr.config.Path, ghalr.handleEvent)
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
	close(ghalr.eventQueue)
	ghalr.wg.Wait()
	if ghalr.server == nil {
		return nil
	}
	ghalr.logger.Warn("Shutting down receiver")
	return ghalr.server.Shutdown(ctx)
}

func (ghalr *githubActionsLogReceiver) handleHealthCheck(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}

func (ghalr *githubActionsLogReceiver) handleEvent(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
		ghalr.wg.Add(1)
		ghalr.eventQueue <- event // send the event to the queue
		w.WriteHeader(http.StatusAccepted)
		//processWorkflowRunEvent(ghalr, w, *event)
	default:
		{
			ghalr.logger.Debug("Skipping the request because it is not a workflow run event")
			w.WriteHeader(http.StatusOK)
		}
	}
}

func (ghalr *githubActionsLogReceiver) processEvents() {
	for event := range ghalr.eventQueue {
		ghalr.logger.Debug("eventQueue size", zap.Int("event_queue_size", len(ghalr.eventQueue)))
		processWorkflowRunEvent(ghalr, *event)
		ghalr.wg.Done()
	}
}

func processWorkflowRunEvent(
	ghalr *githubActionsLogReceiver,
	event github.WorkflowRunEvent,
) {
	ctx := context.Background()
	var withWorkflowInfoFields = func(fields ...zap.Field) []zap.Field {
		workflowInfoFields := []zap.Field{
			zap.String("github.repository", event.GetRepo().GetFullName()),
			zap.String("github.workflow_run.name", event.GetWorkflowRun().GetName()),
			zap.Int64("github.workflow_run.id", event.GetWorkflowRun().GetID()),
			zap.Int("github.workflow_run.run_attempt", event.GetWorkflowRun().GetRunAttempt()),
		}
		return append(workflowInfoFields, fields...)
	}
	if event.GetAction() != "completed" {
		ghalr.logger.Debug("Skipping the request because it is not a completed workflow run", withWorkflowInfoFields()...)
		return
	}
	ghalr.logger.Info("Starting to process webhook event", withWorkflowInfoFields()...)
	_, err := ghalr.convert(ctx, event, withWorkflowInfoFields)
	if err != nil {
		ghalr.logger.Error("Failed to get workflow run data", withWorkflowInfoFields(zap.Error(err))...)
		return
	}
	if err != nil {
		ghalr.logger.Error("Failed to convert Jobs to logs", withWorkflowInfoFields(zap.Error(err))...)
		return
	}
	//err = ghalr.consumeLogsWithRetry(ctx, withWorkflowInfoFields, logs)
	//if err != nil {
	//	ghalr.logger.Error("Failed to consume logs", withWorkflowInfoFields(zap.Error(err))...)
	//	return
	//}
}

func (ghalr *githubActionsLogReceiver) convert(
	ctx context.Context,
	event github.WorkflowRunEvent,
	withWorkflowInfoFields func(fields ...zap.Field) []zap.Field,
) (plog.Logs, error) {
	allWorkflowJobs, err := ghalr.getWorkflowJobs(ctx, event, withWorkflowInfoFields)
	if err != nil {
		return plog.Logs{}, fmt.Errorf("failed to get workflow jobs: %w", err)
	}
	runLogZip, deleteFunc, err := getRunLog(
		ghalr.runLogCache,
		ghalr.logger,
		ctx, ghalr.ghClient,
		http.DefaultClient,
		event.GetRepo(),
		event.GetWorkflowRun(),
	)
	if err != nil {
		return plog.Logs{}, fmt.Errorf("failed to get run log: %w", err)
	}
	defer func() {
		if err := runLogZip.Close(); err != nil {
			ghalr.logger.Warn("Failed to close run log zip", withWorkflowInfoFields(zap.Error(err))...)
		}
		if err := deleteFunc(); err != nil {
			ghalr.logger.Warn("Failed to delete run log zip", withWorkflowInfoFields(zap.Error(err))...)
		}
	}()
	jobs := mapJobs(allWorkflowJobs)
	attachRunLog(&runLogZip.Reader, jobs)
	run := mapRun(event.GetWorkflowRun())
	repository := mapRepository(event.GetRepo())
	ghalr.wg2.Add(2)
	result, err := toLogs(ghalr, repository, run, jobs)
	ghalr.wg2.Done()
	return result, err
}

func (ghalr *githubActionsLogReceiver) getWorkflowJobs(
	ctx context.Context,
	event github.WorkflowRunEvent,
	withWorkflowInfoFields func(fields ...zap.Field) []zap.Field,
) ([]*github.WorkflowJob, error) {
	listWorkflowJobsOpts := &github.ListOptions{
		PerPage: 100,
	}
	var allWorkflowJobs []*github.WorkflowJob
	for {
		workflowJobs, response, err := ghalr.ghClient.Actions.ListWorkflowJobsAttempt(
			ctx,
			event.GetRepo().GetOwner().GetLogin(),
			event.GetRepo().GetName(),
			event.GetWorkflowRun().GetID(),
			int64(event.GetWorkflowRun().GetRunAttempt()),
			listWorkflowJobsOpts,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get workflow Jobs: %w", err)
		}
		allWorkflowJobs = append(allWorkflowJobs, workflowJobs.Jobs...)
		if response.NextPage == 0 {
			limit, _ := strconv.Atoi(response.Header.Get("X-RateLimit-Limit"))
			remaining, _ := strconv.Atoi(response.Header.Get("X-RateLimit-Remaining"))
			reset, _ := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64)
			ghalr.logger.Info(
				"GitHub Api Rate limits",
				withWorkflowInfoFields(
					zap.Int("github.api.rate-limit.core.limit", limit),
					zap.Int("github.api.rate-limit.core.remaining", remaining),
					zap.Time("github.api.rate-limit.core.reset", time.Unix(reset, 0)),
				)...,
			)
			break
		}
		listWorkflowJobsOpts.Page = response.NextPage
	}
	return allWorkflowJobs, nil
}

func (ghalr *githubActionsLogReceiver) consumeLogsWithRetry(ctx context.Context, withWorkflowInfoFields func(fields ...zap.Field) []zap.Field, logs plog.Logs) error {
	expBackoff := backoff.ExponentialBackOff{
		MaxElapsedTime:      ghalr.config.Retry.MaxElapsedTime,
		InitialInterval:     ghalr.config.Retry.InitialInterval,
		MaxInterval:         ghalr.config.Retry.MaxInterval,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	expBackoff.Reset()
	retryableErr := consumererror.Logs{}
	for {
		err := ghalr.consumer.ConsumeLogs(ctx, logs)
		if err == nil {
			ghalr.logger.Debug("Successfully consumed logs", withWorkflowInfoFields(zap.Int("log_record_count", logs.LogRecordCount()))...)
			return nil
		}
		if consumererror.IsPermanent(err) {
			ghalr.logger.Error(
				"Consuming logs failed. The error is not retryable. Dropping data.",
				withWorkflowInfoFields(
					zap.Error(err),
					zap.Int("dropped_items", logs.LogRecordCount()),
				)...,
			)
			return err
		}
		if errors.As(err, &retryableErr) {
			logs = retryableErr.Data()
		}
		backoffDelay := expBackoff.NextBackOff()
		if backoffDelay == backoff.Stop {
			ghalr.logger.Error(
				"Max elapsed time expired. Dropping data.",
				withWorkflowInfoFields(
					zap.Error(err),
					zap.Int("dropped_items", logs.LogRecordCount()),
				)...,
			)
			return err
		}
		ghalr.logger.Debug(
			"Consuming logs failed. Will retry the request after interval.",
			withWorkflowInfoFields(
				zap.Error(err),
				zap.String("interval", backoffDelay.String()),
				zap.Int("logs_count", logs.LogRecordCount()),
			)...,
		)
		select {
		case <-ctx.Done():
			return fmt.Errorf("context is cancelled or timed out %w", err)
		case <-time.After(backoffDelay):
		}
	}
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

type DeleteFunc func() error

func getRunLog(
	cache runLogCache,
	logger *zap.Logger,
	ctx context.Context,
	ghClient *github.Client,
	httpClient *http.Client,
	repository *github.Repository,
	workflowRun *github.WorkflowRun,
) (*zip.ReadCloser, DeleteFunc, error) {
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
