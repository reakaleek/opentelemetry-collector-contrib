package githubactionslogreceiver

import (
	"bufio"
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"strings"
	"time"
)

func toLogs(ghalr *githubActionsLogReceiver, repository Repository, run Run, jobs []Job) (plog.Logs, error) {
	ch := make(chan plog.Logs, 10000)
	signal := make(chan bool)
	timeout := time.NewTimer(2 * time.Second)
	go func() {
		for {
			select {
			case <-timeout.C: // Timeout occurred
				signal <- true
				return // Exit goroutine after timeout
			}
		}
	}()
	go func() {
		for {
			<-signal // Wait for signal
			// Process elements from the channel (similar to previous examples)
			for i := 0; len(ch) > 0; i++ {
				item := <-ch
				err := ghalr.consumeLogsWithRetry(context.Background(), func(fields ...zap.Field) []zap.Field {
					return make([]zap.Field, 0)
				}, item)
				if err != nil {
					ghalr.logger.Error("Failed to consume logs", zap.Error(err))
					return
				}
			}
			ghalr.wg2.Done()
		}
	}()
	for _, job := range jobs {
		for _, step := range job.Steps {
			if step.Log == nil {
				continue
			}
			if err := func() error {
				f, err := step.Log.Open()
				if err != nil {
					return err
				}
				defer f.Close()
				scanner := bufio.NewScanner(f)
				var previousLogRecord *plog.LogRecord
				count := 0
				for scanner.Scan() {
					line := scanner.Text()
					if line == "" {
						continue
					}
					logs := plog.NewLogs()
					resourceLogs := logs.ResourceLogs().AppendEmpty()
					resourceAttributes := resourceLogs.Resource().Attributes()
					resourceAttributes.PutStr("service.name", fmt.Sprintf("github-actions-%s-%s", repository.Org, repository.Name))
					scopeLogsSlice := resourceLogs.ScopeLogs()
					scopeLogs := scopeLogsSlice.AppendEmpty()
					scopeLogs.Scope().SetName("githubactionslogreceiver")
					logRecords := scopeLogs.LogRecords()
					if !startsWithTimestamp(line) {
						if previousLogRecord != nil {
							appendLineToLogRecordBody(previousLogRecord, line)
						}
						continue
					}
					logRecord := logRecords.AppendEmpty()
					logLine, err := parseLogLine(scanner.Text())
					if err != nil {
						return fmt.Errorf("failed to parse log line: %w", err)
					}
					if err := attachData(&logRecord, repository, run, job, step, logLine); err != nil {
						return fmt.Errorf("failed to attach data to log record: %w", err)
					}

					ch <- logs
					count++
					if count == 10 {
						signal <- true // Send signal after adding 10 elements
						count = 0      // Reset count for next batch
					}
					if err != nil {
						return err
					}
					previousLogRecord = &logRecord
				}
				return nil
			}(); err != nil {
				return plog.Logs{}, err
			}
		}
	}
	return plog.Logs{}, nil
}

func appendLineToLogRecordBody(logRecord *plog.LogRecord, line string) {
	logRecord.Body().SetStr(logRecord.Body().Str() + "\n" + line)
}

func attachData(logRecord *plog.LogRecord, repository Repository, run Run, job Job, step Step, logLine LogLine) error {
	logRecord.SetSeverityNumber(plog.SeverityNumber(logLine.SeverityNumber))
	if err := attachTraceId(logRecord, run); err != nil {
		return err
	}
	if err := attachSpanId(logRecord, run, job, step); err != nil {
		return err
	}
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(logLine.Timestamp))
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord.Attributes().PutStr("github.repository", repository.FullName)
	logRecord.Body().SetStr(logLine.Body)
	attachRunAttributes(logRecord, run)
	attachJobAttributes(logRecord, job)
	attachStepAttributes(logRecord, step)
	return nil
}

func startsWithTimestamp(line string) bool {
	if line == "" {
		return false
	}
	fields := strings.Fields(line)
	_, err := time.Parse(time.RFC3339Nano, fields[0])
	return err == nil
}

// parseLogLine parses a log line from the GitHub Actions log file
func parseLogLine(line string) (LogLine, error) {
	var severityText string
	var severityNumber = 0 // Unspecified
	parts := strings.SplitN(line, " ", 2)
	extractedTimestamp := parts[0]
	extractedLogMessage := parts[1]
	timestamp, err := time.Parse(time.RFC3339Nano, extractedTimestamp)
	if err != nil {
		return LogLine{}, err
	}
	switch {
	case strings.HasPrefix(extractedLogMessage, "##[debug]"):
		{
			severityNumber = 5
			severityText = "DEBUG"
		}
	case strings.HasPrefix(extractedLogMessage, "##[error]"):
		{
			severityNumber = 17
			severityText = "ERROR"
		}
	}
	return LogLine{
		Body:           extractedLogMessage,
		Timestamp:      timestamp,
		SeverityNumber: severityNumber,
		SeverityText:   severityText,
	}, nil
}

func attachTraceId(logRecord *plog.LogRecord, run Run) error {
	traceId, err := generateTraceID(run.ID, int(run.RunAttempt))
	if err != nil {
		return err
	}
	logRecord.SetTraceID(traceId)
	return nil
}

func attachSpanId(logRecord *plog.LogRecord, run Run, job Job, step Step) error {
	spanId, err := generateStepSpanID(run.ID, int(run.RunAttempt), job.Name, step.Name, int(step.Number))
	if err != nil {
		return err
	}
	logRecord.SetSpanID(spanId)
	return nil
}

func attachRunAttributes(logRecord *plog.LogRecord, run Run) {
	logRecord.Attributes().PutInt("github.workflow_run.id", run.ID)
	logRecord.Attributes().PutStr("github.workflow_run.name", run.Name)
	logRecord.Attributes().PutInt("github.workflow_run.run_attempt", run.RunAttempt)
	logRecord.Attributes().PutInt("github.workflow_run.run_number", run.RunNumber)
	logRecord.Attributes().PutStr("github.workflow_run.url", fmt.Sprintf("%s/attempts/%d", run.URL, run.RunAttempt))
	logRecord.Attributes().PutStr("github.workflow_run.conclusion", run.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_run.status", run.Status)
	logRecord.Attributes().PutStr("github.workflow_run.run_started_at", pcommon.NewTimestampFromTime(run.RunStartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_run.event", run.Event)
	logRecord.Attributes().PutStr("github.workflow_run.created_at", pcommon.NewTimestampFromTime(run.CreatedAt).String())
	logRecord.Attributes().PutStr("github.workflow_run.updated_at", pcommon.NewTimestampFromTime(run.UpdatedAt).String())
	logRecord.Attributes().PutStr("github.workflow_run.actor.login", run.ActorLogin)
	logRecord.Attributes().PutInt("github.workflow_run.actor.id", run.ActorID)
}

func attachJobAttributes(logRecord *plog.LogRecord, job Job) {
	logRecord.Attributes().PutInt("github.workflow_job.id", job.ID)
	logRecord.Attributes().PutStr("github.workflow_job.name", job.Name)
	logRecord.Attributes().PutStr("github.workflow_job.url", job.URL)
	logRecord.Attributes().PutStr("github.workflow_job.started_at", pcommon.NewTimestampFromTime(job.StartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.completed_at", pcommon.NewTimestampFromTime(job.CompletedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.conclusion", job.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_job.status", job.Status)
	logRecord.Attributes().PutInt("github.workflow_job.runner.group_id", job.RunnerGroupID)
	logRecord.Attributes().PutStr("github.workflow_job.runner.group_name", job.RunnerGroupName)
	logRecord.Attributes().PutStr("github.workflow_job.runner.name", job.RunnerName)
}

func attachStepAttributes(logRecord *plog.LogRecord, step Step) {
	logRecord.Attributes().PutStr("github.workflow_job.step.name", step.Name)
	logRecord.Attributes().PutInt("github.workflow_job.step.number", step.Number)
	logRecord.Attributes().PutStr("github.workflow_job.step.started_at", pcommon.NewTimestampFromTime(step.StartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.step.completed_at", pcommon.NewTimestampFromTime(step.CompletedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.step.conclusion", step.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_job.step.status", step.Status)
}
