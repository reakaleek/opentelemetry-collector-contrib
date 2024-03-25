package githubactionslogreceiver

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/go-github/v60/github"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
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
	ghalr.server = &http.Server{Addr: ghalr.config.ServerConfig.Endpoint, Handler: ghalr}
	ghalr.wg.Add(1)
	go func() {
		defer ghalr.wg.Done()
		if err := ghalr.server.ListenAndServe(); errors.Is(err, http.ErrServerClosed) {
			ghalr.logger.Error("Receiver server has been shutdown", zap.Error(err))
		}
	}()
	return nil
}

func (ghalr *githubActionsLogReceiver) Shutdown(ctx context.Context) error {
	var err error
	if ghalr.server != nil {
		err = ghalr.server.Close()
	}
	ghalr.wg.Wait()
	return err
}

func (ghalr *githubActionsLogReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var response GitHubWorkflowRunWebhookPayload
	err := json.NewDecoder(r.Body).Decode(&response)
	if err != nil {
		ghalr.logger.Error("Failed to decode request", zap.Error(err))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ghalr.logger.Info("Received a request", zap.String("path", r.URL.Path))
	ghalr.logger.Debug("Request Payload", zap.Any("response", response))

	if r.URL.Path != ghalr.config.Path {
		ghalr.logger.Info("Received a request to an unknown path", zap.String("path", r.URL.Path))
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if response.Action != "completed" {
		ghalr.logger.Info("Skipping the request because it is no a completed workflow run")
		w.WriteHeader(http.StatusOK)
		return
	}

	ghClient := github.NewClient(nil).WithAuthToken(string(ghalr.config.GitHubToken))
	workflowJobs, _, err := ghClient.Actions.ListWorkflowJobs(r.Context(), response.Repository.GetOwner().GetLogin(), response.Repository.GetName(), response.WorkflowRun.GetID(), nil)
	if err != nil {
		ghalr.logger.Error("Failed to get workflow Jobs", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	ghalr.logger.Info("Received Jobs", zap.Any("Jobs", workflowJobs))

	runLogZip, err := ghalr.getRunLog(r.Context(), ghClient, http.DefaultClient, response.Repository, response.WorkflowRun)
	if err != nil {
		ghalr.logger.Error("Failed to get run log", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer runLogZip.Close()
	jobs := make([]Job, len(workflowJobs.Jobs))
	for i, job := range workflowJobs.Jobs {
		jobs[i] = mapJob(job)
	}

	rateLimit, _, err := ghClient.RateLimit.Get(r.Context())

	ghalr.logger.Info("Rate limit", zap.Any("rate_limit", rateLimit.GetCore()))
	if err != nil {
		return
	}
	attachRunLog(&runLogZip.Reader, jobs)
	run := Run{
		Repository: Repository{FullName: response.Repository.GetFullName()},
		Workflow:   Workflow{Name: response.Workflow.GetName()},
		Jobs:       jobs,
		RunAttempt: int64(response.WorkflowRun.GetRunAttempt()),
	}
	logs, err := runToLogs(ghalr.logger, run)
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

func runToLogs(logger *zap.Logger, run Run) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceAttributes := resourceLogs.Resource().Attributes()
	resourceAttributes.PutStr("repository", run.Repository.FullName)
	resourceAttributes.PutInt("workflow_run.run_attempt", run.RunAttempt)
	resourceAttributes.PutInt("workflow_run.id", run.Jobs[0].RunID)
	for _, job := range run.Jobs {
		scopeLogsSlice := logs.ResourceLogs().AppendEmpty().ScopeLogs()
		scopeLogs := scopeLogsSlice.AppendEmpty()
		logRecords := scopeLogs.LogRecords()
		for _, step := range job.Steps {
			if step.Log == nil {
				continue
			}
			f, err := step.Log.Open()
			if err != nil {
				return logs, err
			}
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				logRecord := logRecords.AppendEmpty()
				parts := strings.SplitN(scanner.Text(), " ", 2)
				extractedTimestamp := parts[0]
				extractedLogMessage := parts[1]
				var timestamp time.Time
				timestamp, err = time.Parse(time.RFC3339Nano, extractedTimestamp)
				if err != nil {
					return logs, fmt.Errorf("failed to parse timestamp: %w", err)
				}
				if strings.HasPrefix(extractedLogMessage, "#[debug]") {
					logRecord.SetSeverityNumber(5) // DEBUG
				}
				logRecord.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
				logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				logRecord.Attributes().PutStr("github.repository", run.Repository.FullName)
				logRecord.Attributes().PutStr("github.workflow.name", run.Workflow.Name)
				logRecord.Attributes().PutInt("github.workflow_run.id", job.RunID)
				logRecord.Attributes().PutInt("github.workflow_run.run_attempt", run.RunAttempt)
				logRecord.Attributes().PutInt("github.workflow_job.id", job.ID)
				logRecord.Attributes().PutStr("github.workflow_job.job.name", job.Name)
				logRecord.Attributes().PutStr("github.workflow_job.job.url", job.URL)
				logRecord.Attributes().PutStr("github.workflow_job.started_at", pcommon.NewTimestampFromTime(job.StartedAt).String())
				logRecord.Attributes().PutStr("github.workflow_job.completed_at", pcommon.NewTimestampFromTime(job.CompletedAt).String())
				logRecord.Attributes().PutStr("github.workflow_job.conclusion", string(job.Conclusion))
				logRecord.Attributes().PutStr("github.workflow_job.status", string(job.Status))

				logRecord.Attributes().PutStr("github.workflow_job.step.name", step.Name)
				logRecord.Attributes().PutInt("github.workflow_job.step.number", int64(step.Number))
				logRecord.Attributes().PutStr("github.workflow_job.step.conclusion", string(step.Conclusion))
				logRecord.Attributes().PutStr("github.workflow_job.step.status", string(step.Status))
				logRecord.Attributes().PutStr("github.workflow_job.step.status", string(step.Status))
				logRecord.Body().SetStr(extractedLogMessage)
			}
		}
	}
	return logs, nil
}

func getLog(httpClient *http.Client, logURL string) (io.ReadCloser, error) {
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

func (ghalr *githubActionsLogReceiver) getRunLog(ctx context.Context, ghClient *github.Client, httpClient *http.Client, repository github.Repository, workflowRun github.WorkflowRun) (*zip.ReadCloser, error) {
	cache := ghalr.runLogCache
	filename := fmt.Sprintf("run-log-%d-%d.zip", workflowRun.ID, workflowRun.GetRunStartedAt().Unix())
	fp := filepath.Join(os.TempDir(), "run-log-cache", filename)
	ghalr.logger.Info("Checking if log exists in cache", zap.String("path", fp))
	if !cache.Exists(fp) {
		logURL, _, err := ghClient.Actions.GetWorkflowRunLogs(ctx, repository.GetOwner().GetLogin(), repository.GetName(), workflowRun.GetID(), 4)
		ghalr.logger.Info("Received logs url", zap.Any("url", logURL))
		if err != nil {
			ghalr.logger.Error("Failed to get logs url", zap.Error(err))
			return nil, err
		}
		response, err := getLog(httpClient, logURL.String())
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

func mapJob(job *github.WorkflowJob) Job {
	steps := make([]Step, len(job.Steps))
	for _, s := range job.Steps {
		steps = append(steps, Step{
			Name:        *s.Name,
			Status:      Status(*s.Status),
			Conclusion:  Conclusion(*s.Conclusion),
			Number:      int(*s.Number),
			StartedAt:   s.GetStartedAt().Time,
			CompletedAt: s.GetCompletedAt().Time,
		})
	}
	Steps(steps).Sort()
	return Job{
		ID:          job.GetID(),
		Status:      Status(job.GetStatus()),
		Conclusion:  Conclusion(job.GetConclusion()),
		Name:        job.GetName(),
		StartedAt:   job.GetStartedAt().Time,
		CompletedAt: job.GetCompletedAt().Time,
		URL:         job.GetHTMLURL(),
		RunID:       job.GetRunID(),
		Steps:       steps,
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
