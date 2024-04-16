package githubactionslogreceiver

import (
	"bufio"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"strings"
	"time"
)

func toLogs(repository Repository, run Run, jobs []Job) (plog.Logs, error) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceAttributes := resourceLogs.Resource().Attributes()
	resourceAttributes.PutStr("service.name", fmt.Sprintf("github-actions-%s-%s", repository.Org, repository.Name))
	for _, job := range jobs {
		scopeLogsSlice := resourceLogs.ScopeLogs()
		scopeLogs := scopeLogsSlice.AppendEmpty()
		scopeLogs.Scope().SetName("github-actions")
		logRecords := scopeLogs.LogRecords()
		for _, step := range job.Steps {
			if step.Log == nil {
				continue
			}
			err := processStep(repository, run, job, step, &logRecords)
			if err != nil {
				return plog.Logs{}, fmt.Errorf("failed to process step: %w", err)
			}
		}
	}
	return logs, nil
}

func processStep(repository Repository, run Run, job Job, step Step, logRecords *plog.LogRecordSlice) error {
	f, err := step.Log.Open()
	if err != nil {
		return fmt.Errorf("unable to open log file: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var previousLogRecord *plog.LogRecord
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
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
		if err := attachData(previousLogRecord, &logRecord, repository, run, job, step, logLine); err != nil {
			return fmt.Errorf("failed to attach data to log record: %w", err)
		}
		previousLogRecord = &logRecord
	}
	return nil
}

func appendLineToLogRecordBody(logRecord *plog.LogRecord, line string) {
	logRecord.Body().SetStr(logRecord.Body().Str() + "\n" + line)
}

func attachData(previousLogRecord *plog.LogRecord, logRecord *plog.LogRecord, repository Repository, run Run, job Job, step Step, logLine LogLine) error {
	logRecord.SetSeverityNumber(plog.SeverityNumber(logLine.SeverityNumber))
	if err := attachTraceId(logRecord, run); err != nil {
		return err
	}
	if err := attachSpanId(logRecord, run, job, step); err != nil {
		return err
	}
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(logLine.Timestamp))
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord.Body().SetStr(logLine.Body)

	if previousLogRecord != nil {
		previousLogRecord.Attributes().CopyTo(logRecord.Attributes())
		return nil
	}

	return logRecord.Attributes().FromRaw(map[string]interface{}{
		"github.repository": repository.FullName,

		"github.workflow_run.id":             run.ID,
		"github.workflow_run.name":           run.Name,
		"github.workflow_run.run_attempt":    run.RunAttempt,
		"github.workflow_run.run_number":     run.RunNumber,
		"github.workflow_run.url":            fmt.Sprintf("%s/attempts/%d", run.URL, run.RunAttempt),
		"github.workflow_run.conclusion":     run.Conclusion,
		"github.workflow_run.status":         run.Status,
		"github.workflow_run.run_started_at": pcommon.NewTimestampFromTime(run.RunStartedAt).String(),
		"github.workflow_run.event":          run.Event,
		"github.workflow_run.created_at":     pcommon.NewTimestampFromTime(run.CreatedAt).String(),
		"github.workflow_run.updated_at":     pcommon.NewTimestampFromTime(run.UpdatedAt).String(),
		"github.workflow_run.actor.login":    run.ActorLogin,
		"github.workflow_run.actor.id":       run.ActorID,

		"github.workflow_job.id":           job.ID,
		"github.workflow_job.name":         job.Name,
		"github.workflow_job.url":          job.URL,
		"github.workflow_job.started_at":   pcommon.NewTimestampFromTime(job.StartedAt).String(),
		"github.workflow_job.completed_at": pcommon.NewTimestampFromTime(job.CompletedAt).String(),
		"github.workflow_job.conclusion":   job.Conclusion,
		"github.workflow_job.status":       job.Status,

		"github.workflow_job.step.name":         step.Name,
		"github.workflow_job.step.number":       step.Number,
		"github.workflow_job.step.started_at":   pcommon.NewTimestampFromTime(step.StartedAt).String(),
		"github.workflow_job.step.completed_at": pcommon.NewTimestampFromTime(step.CompletedAt).String(),
		"github.workflow_job.step.conclusion":   step.Conclusion,
		"github.workflow_job.step.status":       step.Status,
	})
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

func attachRunAttributes(logRecord *plog.LogRecord, run Run) error {
	return logRecord.Attributes().FromRaw(map[string]interface{}{
		"github.workflow_run.id":             run.ID,
		"github.workflow_run.name":           run.Name,
		"github.workflow_run.run_attempt":    run.RunAttempt,
		"github.workflow_run.run_number":     run.RunNumber,
		"github.workflow_run.url":            fmt.Sprintf("%s/attempts/%d", run.URL, run.RunAttempt),
		"github.workflow_run.conclusion":     run.Conclusion,
		"github.workflow_run.status":         run.Status,
		"github.workflow_run.run_started_at": pcommon.NewTimestampFromTime(run.RunStartedAt).String(),
		"github.workflow_run.event":          run.Event,
		"github.workflow_run.created_at":     pcommon.NewTimestampFromTime(run.CreatedAt).String(),
		"github.workflow_run.updated_at":     pcommon.NewTimestampFromTime(run.UpdatedAt).String(),
		"github.workflow_run.actor.login":    run.ActorLogin,
		"github.workflow_run.actor.id":       run.ActorID,
	})
}

func attachJobAttributes(logRecord *plog.LogRecord, job Job) error {
	return logRecord.Attributes().FromRaw(map[string]interface{}{
		"github.workflow_job.id":           job.ID,
		"github.workflow_job.name":         job.Name,
		"github.workflow_job.url":          job.URL,
		"github.workflow_job.started_at":   pcommon.NewTimestampFromTime(job.StartedAt).String(),
		"github.workflow_job.completed_at": pcommon.NewTimestampFromTime(job.CompletedAt).String(),
		"github.workflow_job.conclusion":   job.Conclusion,
		"github.workflow_job.status":       job.Status,
	})
}

func attachStepAttributes(logRecord *plog.LogRecord, step Step) error {
	return logRecord.Attributes().FromRaw(map[string]interface{}{
		"github.workflow_job.step.name":         step.Name,
		"github.workflow_job.step.number":       step.Number,
		"github.workflow_job.step.started_at":   pcommon.NewTimestampFromTime(step.StartedAt).String(),
		"github.workflow_job.step.completed_at": pcommon.NewTimestampFromTime(step.CompletedAt).String(),
		"github.workflow_job.step.conclusion":   step.Conclusion,
		"github.workflow_job.step.status":       step.Status,
	})
	//logRecord.Attributes().PutStr("github.workflow_job.step.name", step.Name)
	//logRecord.Attributes().PutInt("github.workflow_job.step.number", step.Number)
	//logRecord.Attributes().PutStr("github.workflow_job.step.started_at", pcommon.NewTimestampFromTime(step.StartedAt).String())
	//logRecord.Attributes().PutStr("github.workflow_job.step.completed_at", pcommon.NewTimestampFromTime(step.CompletedAt).String())
	//logRecord.Attributes().PutStr("github.workflow_job.step.conclusion", step.Conclusion)
	//logRecord.Attributes().PutStr("github.workflow_job.step.status", step.Status)
}
