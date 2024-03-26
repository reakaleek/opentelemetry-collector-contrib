package githubactionslogreceiver

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/go-github/v60/github"
	"github.com/julienschmidt/httprouter"
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
	jobs := mapJobs(workflowJobs.Jobs)
	attachRunLog(&runLogZip.Reader, jobs)
	run := mapRun(&response.WorkflowRun)
	repository := mapRepository(&response.Repository)
	logs, err := ghalr.toLogs(repository, run, jobs)
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

func (ghalr *githubActionsLogReceiver) toLogs(repository Repository, run Run, jobs []Job) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceAttributes := resourceLogs.Resource().Attributes()
	resourceAttributes.PutStr("repository", repository.FullName)
	resourceAttributes.PutInt("workflow_run.run_attempt", run.RunAttempt)
	resourceAttributes.PutInt("workflow_run.id", run.ID)
	for _, job := range jobs {
		scopeLogsSlice := logs.ResourceLogs().AppendEmpty().ScopeLogs()
		scopeLogs := scopeLogsSlice.AppendEmpty()
		logRecords := scopeLogs.LogRecords()
		for _, step := range job.Steps {
			if step.Log == nil {
				ghalr.logger.Warn(fmt.Sprintf("The step %s did not have any logs", step.Name), zap.Any("run", run), zap.Any("step", step))
				continue
			}
			f, err := step.Log.Open()
			if err != nil {
				return logs, err
			}
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				logRecord := logRecords.AppendEmpty()

				logLine, err := parseLogLine(scanner.Text())
				if err != nil {
					return logs, fmt.Errorf("failed to parse log line: %w", err)
				}

				if strings.HasPrefix(logLine.Body, "#[debug]") {
					logRecord.SetSeverityNumber(5) // DEBUG
				}

				if err = setTraceAndSpanId(&logRecord, &run, &job, &step); err != nil {
					return logs, err
				}

				logRecord.SetTimestamp(pcommon.NewTimestampFromTime(logLine.Timestamp))
				logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))

				logRecord.Attributes().PutStr("github.repository", repository.FullName)
				logRecord.Body().SetStr(logLine.Body)

				addRunAttributes(&logRecord, &run)
				addJobAttributes(&logRecord, &job)
				addStepAttributes(&logRecord, &step)
			}
		}
	}
	return logs, nil
}

func parseLogLine(line string) (*LogLine, error) {
	parts := strings.SplitN(line, " ", 2)
	extractedTimestamp := parts[0]
	extractedLogMessage := parts[1]
	timestamp, err := time.Parse(time.RFC3339Nano, extractedTimestamp)
	if err != nil {
		return &LogLine{}, err
	}
	return &LogLine{
		Body:      extractedLogMessage,
		Timestamp: timestamp,
	}, nil
}

func setTraceAndSpanId(logRecord *plog.LogRecord, run *Run, job *Job, step *Step) error {
	traceId, err := generateTraceID(run.ID, int(run.RunAttempt))
	if err != nil {
		return err
	}
	logRecord.SetTraceID(traceId)
	spanId, err := generateStepSpanID(run.ID, int(run.RunAttempt), job.Name, step.Name, int(step.Number))
	if err != nil {
		return err
	}
	logRecord.SetSpanID(spanId)
	return nil
}

func addRunAttributes(logRecord *plog.LogRecord, run *Run) {
	logRecord.Attributes().PutInt("github.workflow_run.id", run.ID)
	logRecord.Attributes().PutStr("github.workflow_run.name", run.Name)
	logRecord.Attributes().PutInt("github.workflow_run.run_attempt", run.RunAttempt)
	logRecord.Attributes().PutInt("github.workflow_run.run_number", run.RunNumber)
	logRecord.Attributes().PutStr("github.workflow_run.url", fmt.Sprintf("%s/attempts/%d", run.URL, run.RunAttempt))
	logRecord.Attributes().PutStr("github.workflow_run.conclusion", run.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_run.status", run.Status)
	logRecord.Attributes().PutStr("github.workflow_run.run_started_at", pcommon.NewTimestampFromTime(run.RunStartedAt).String())
}

func addJobAttributes(logRecord *plog.LogRecord, job *Job) {
	logRecord.Attributes().PutInt("github.workflow_job.id", job.ID)
	logRecord.Attributes().PutStr("github.workflow_job.name", job.Name)
	logRecord.Attributes().PutStr("github.workflow_job.url", job.URL)
	logRecord.Attributes().PutStr("github.workflow_job.started_at", pcommon.NewTimestampFromTime(job.StartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.completed_at", pcommon.NewTimestampFromTime(job.CompletedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.conclusion", job.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_job.status", job.Status)
}

func addStepAttributes(logRecord *plog.LogRecord, step *Step) {
	logRecord.Attributes().PutStr("github.workflow_job.step.name", step.Name)
	logRecord.Attributes().PutInt("github.workflow_job.step.number", step.Number)
	logRecord.Attributes().PutStr("github.workflow_job.step.started_at", pcommon.NewTimestampFromTime(step.StartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.step.completed_at", pcommon.NewTimestampFromTime(step.CompletedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.step.conclusion", step.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_job.step.status", step.Status)
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

// generateTraceID generates a trace ID from the run ID and run attempt
// Copied from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/b45f1739c29039a0314b9c97c3ccc578562df36f/receiver/githubactionsreceiver/trace_event_handling.go#L212
// to be able to correlate logs with traces
func generateTraceID(runID int64, runAttempt int) (pcommon.TraceID, error) {
	input := fmt.Sprintf("%d%dt", runID, runAttempt)
	hash := sha256.Sum256([]byte(input))
	traceIDHex := hex.EncodeToString(hash[:])

	var traceID pcommon.TraceID
	_, err := hex.Decode(traceID[:], []byte(traceIDHex[:32]))
	if err != nil {
		return pcommon.TraceID{}, err
	}

	return traceID, nil
}

// generateStepSpanID generates a span ID from the run ID, run attempt, job name, step name and step number
// Copied from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/b45f1739c29039a0314b9c97c3ccc578562df36f/receiver/githubactionsreceiver/trace_event_handling.go#L262
// to be able to correlate logs with traces
func generateStepSpanID(runID int64, runAttempt int, jobName, stepName string, stepNumber ...int) (pcommon.SpanID, error) {
	var input string
	if len(stepNumber) > 0 && stepNumber[0] > 0 {
		input = fmt.Sprintf("%d%d%s%s%d", runID, runAttempt, jobName, stepName, stepNumber[0])
	} else {
		input = fmt.Sprintf("%d%d%s%s", runID, runAttempt, jobName, stepName)
	}
	hash := sha256.Sum256([]byte(input))
	spanIDHex := hex.EncodeToString(hash[:])

	var spanID pcommon.SpanID
	_, err := hex.Decode(spanID[:], []byte(spanIDHex[16:32]))
	if err != nil {
		return pcommon.SpanID{}, err
	}
	return spanID, nil
}
