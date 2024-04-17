package githubactionslogreceiver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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
	"strings"
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
	if ghalr.server == nil {
		return nil
	}
	ghalr.logger.Error("Shutting down receiver", zap.Error(ctx.Err()))
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
		ctx := context.WithoutCancel(r.Context())
		handleWorkflowRunEvent(ctx, ghalr, w, *event)
	default:
		{
			ghalr.logger.Debug("Skipping the request because it is not a workflow run event")
			w.WriteHeader(http.StatusOK)
		}
	}
}

func handleWorkflowRunEvent(
	ctx context.Context,
	ghalr *githubActionsLogReceiver,
	w http.ResponseWriter,
	event github.WorkflowRunEvent,
) {
	ctx = context.WithValue(ctx, "event", event)
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
		w.WriteHeader(http.StatusOK)
		return
	}
	ghalr.logger.Info("Starting to process webhook event", withWorkflowInfoFields()...)
	rateLimit, err := ghalr.processWorkflowRunEvent(ctx, withWorkflowInfoFields, event)
	if err != nil {
		ghalr.logger.Error("Failed to get workflow run data", withWorkflowInfoFields(zap.Error(err))...)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ghalr.logger.Info(
		"GitHub Api Rate limits",
		withWorkflowInfoFields(
			zap.Int("github.api.rate-limit.core.limit", rateLimit.limit),
			zap.Int("github.api.rate-limit.core.remaining", rateLimit.remaining),
			zap.Time("github.api.rate-limit.core.reset", rateLimit.reset),
		)...,
	)
}

func (ghalr *githubActionsLogReceiver) processWorkflowRunEvent(
	ctx context.Context,
	withWorkflowInfoFields func(fields ...zap.Field) []zap.Field,
	event github.WorkflowRunEvent,
) (githubRateLimit, error) {
	allWorkflowJobs, rateLimit, err := getWorkflowJobs(ctx, event, ghalr.ghClient)
	if err != nil {
		return githubRateLimit{}, fmt.Errorf("failed to get workflow jobs: %w", err)
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
		return rateLimit, fmt.Errorf("failed to get run log: %w", err)
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
	err = ghalr.batch(ctx, repository, run, jobs, withWorkflowInfoFields)
	if err != nil {
		return rateLimit, err
	}
	return rateLimit, nil
}

func (ghalr *githubActionsLogReceiver) batch(ctx context.Context, repository Repository, run Run, jobs []Job, withWorkflowInfoFields func(fields ...zap.Field) []zap.Field) error {
	for _, job := range jobs {
		for _, step := range job.Steps {
			if step.Log == nil {
				continue
			}
			err := func() error {
				f, err := step.Log.Open()
				if err != nil {
					return err
				}
				defer f.Close()
				scanner := bufio.NewScanner(f)
				batchSize := ghalr.config.BatchSize
				batch := make([]string, 0, batchSize)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.TrimSpace(line) == "" {
						continue
					}
					if !startsWithTimestamp(line) {
						batchLen := len(batch)
						if batchLen > 0 {
							batch[batchLen-1] += "\n" + line
						}
						continue
					}
					if len(batch) == batchSize {
						err := ghalr.processBatch(ctx, withWorkflowInfoFields, batch, repository, run, job, step)
						if err != nil {
							return err
						}
						batch = batch[:0]
					}
					batch = append(batch, line)
				}
				if len(batch) > 0 {
					return ghalr.processBatch(ctx, withWorkflowInfoFields, batch, repository, run, job, step)
				}
				return nil
			}()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ghalr *githubActionsLogReceiver) processBatch(ctx context.Context, withWorkflowInfoFields func(fields ...zap.Field) []zap.Field, batch []string, repository Repository, run Run, job Job, step Step) error {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceAttributes := resourceLogs.Resource().Attributes()
	resourceAttributes.PutStr("service.name", fmt.Sprintf("github-actions-%s-%s", repository.Org, repository.Name))
	scopeLogsSlice := resourceLogs.ScopeLogs()
	scopeLogs := scopeLogsSlice.AppendEmpty()
	scopeLogs.Scope().SetName("github-actions")
	logRecords := scopeLogs.LogRecords()
	for _, line := range batch {
		if !startsWithTimestamp(line) {
			ghalr.logger.Warn("TODO: Skipping line because it does not start with a timestamp", zap.String("line", line))
			continue
		}
		logLine, err := parseLogLine(line)
		if err != nil {
			ghalr.logger.Error("Failed to parse log line", zap.Error(err))
			continue
		}
		logRecord := logRecords.AppendEmpty()
		if err := attachData(&logRecord, repository, run, job, step, logLine); err != nil {
			ghalr.logger.Error("Failed to attach data to log record", zap.Error(err))
		}
	}
	if logs.LogRecordCount() == 0 {
		return nil
	}
	return ghalr.consumeLogsWithRetry(ctx, withWorkflowInfoFields, logs)
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
			ghalr.logger.Info("Successfully consumed logs", withWorkflowInfoFields(zap.Int("log_record_count", logs.LogRecordCount()))...)
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
