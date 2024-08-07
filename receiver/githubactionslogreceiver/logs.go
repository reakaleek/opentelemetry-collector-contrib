package githubactionslogreceiver

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"strings"
	"time"
)

func attachData(logRecord *plog.LogRecord, repository Repository, run Run, job Job, step Step, logLine LogLine) error {
	logRecord.SetSeverityNumber(plog.SeverityNumber(logLine.SeverityNumber))
	logRecord.SetSeverityText(logLine.SeverityText)
	if err := attachTraceId(logRecord, run); err != nil {
		return err
	}
	if err := attachSpanId(logRecord, run, job, step); err != nil {
		return err
	}
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(logLine.Timestamp))
	logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	logRecord.Body().SetStr(logLine.Body)
	attachRepositoryAttributes(logRecord, repository)
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
	if len(fields) == 0 {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, fields[0])
	return err == nil
}

// parseLogLine parses a log line from the GitHub Actions log file
func parseLogLine(line string) (LogLine, error) {
	var severityText string
	var severityNumber = 0 // Unspecified
	before, after, found := strings.Cut(line, " ")
	if !found {
		return LogLine{}, fmt.Errorf("could not parse log line: %s", line)
	}
	extractedTimestamp := before
	extractedLogMessage := after
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
	spanId, err := generateStepSpanID(run.ID, int(run.RunAttempt), job.Name, step.Name, step.Number)
	if err != nil {
		return err
	}
	logRecord.SetSpanID(spanId)
	return nil
}

func attachRepositoryAttributes(logRecord *plog.LogRecord, repository Repository) {
	logRecord.Attributes().PutStr("github.repository", repository.FullName)
}

func attachRunAttributes(logRecord *plog.LogRecord, run Run) {
	logRecord.Attributes().PutInt("github.workflow_run.id", run.ID)
	logRecord.Attributes().PutStr("github.workflow_run.name", run.Name)
	logRecord.Attributes().PutInt("github.workflow_run.run_attempt", run.RunAttempt)
	logRecord.Attributes().PutStr("github.workflow_run.conclusion", run.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_run.status", run.Status)
	logRecord.Attributes().PutStr("github.workflow_run.run_started_at", pcommon.NewTimestampFromTime(run.RunStartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_run.event", run.Event)
	logRecord.Attributes().PutStr("github.workflow_run.created_at", pcommon.NewTimestampFromTime(run.CreatedAt).String())
	logRecord.Attributes().PutStr("github.workflow_run.updated_at", pcommon.NewTimestampFromTime(run.UpdatedAt).String())
	logRecord.Attributes().PutStr("github.workflow_run.actor.login", run.ActorLogin)
	logRecord.Attributes().PutInt("github.workflow_run.actor.id", run.ActorID)
	logRecord.Attributes().PutStr("github.workflow_run.head_branch", run.HeadBranch)
}

func attachJobAttributes(logRecord *plog.LogRecord, job Job) {
	logRecord.Attributes().PutInt("github.workflow_job.id", job.ID)
	logRecord.Attributes().PutStr("github.workflow_job.name", job.Name)
	logRecord.Attributes().PutStr("github.workflow_job.started_at", pcommon.NewTimestampFromTime(job.StartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.completed_at", pcommon.NewTimestampFromTime(job.CompletedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.conclusion", job.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_job.status", job.Status)
}

func attachStepAttributes(logRecord *plog.LogRecord, step Step) {
	logRecord.Attributes().PutStr("github.workflow_job.step.name", step.Name)
	logRecord.Attributes().PutInt("github.workflow_job.step.number", step.Number)
	logRecord.Attributes().PutStr("github.workflow_job.step.started_at", pcommon.NewTimestampFromTime(step.StartedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.step.completed_at", pcommon.NewTimestampFromTime(step.CompletedAt).String())
	logRecord.Attributes().PutStr("github.workflow_job.step.conclusion", step.Conclusion)
	logRecord.Attributes().PutStr("github.workflow_job.step.status", step.Status)
}
