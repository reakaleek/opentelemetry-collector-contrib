package githubactionslogreceiver

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"testing"
	"time"
)

func assertAttributeEquals(t *testing.T, attributes pcommon.Map, key string, expected pcommon.Value) {
	actual, _ := attributes.Get(key)
	assert.Equal(t, expected, actual)
}

func TestAttachRepositoryAttributes(t *testing.T) {
	// arrange
	repository := Repository{
		FullName: "org/repo",
	}
	logRecord := plog.NewLogRecord()

	// act
	attachRepositoryAttributes(&logRecord, repository)

	// assert
	assert.Equal(t, 1, logRecord.Attributes().Len())
	assertAttributeEquals(t, logRecord.Attributes(), "github.repository", pcommon.NewValueStr("org/repo"))
}

func TestAttachRunAttributes(t *testing.T) {
	// arrange
	run := Run{
		ID:           1,
		Name:         "Run Name",
		RunAttempt:   1,
		RunNumber:    1,
		RunStartedAt: time.Now(),
		URL:          "https://example.com",
		Status:       "complete",
		Conclusion:   "success",
		Event:        "push",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now().Add(time.Duration(60)),
		ActorLogin:   "reakaleek",
		ActorID:      1,
		HeadBranch:   "main",
	}
	logRecord := plog.NewLogRecord()

	// act
	attachRunAttributes(&logRecord, run)

	// assert
	assert.Equal(t, 12, logRecord.Attributes().Len())
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.id", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.name", pcommon.NewValueStr("Run Name"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.run_attempt", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.conclusion", pcommon.NewValueStr("success"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.run_started_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(run.RunStartedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.status", pcommon.NewValueStr("complete"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.event", pcommon.NewValueStr("push"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.created_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(run.CreatedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.updated_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(run.UpdatedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.actor.login", pcommon.NewValueStr("reakaleek"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.actor.id", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.head_branch", pcommon.NewValueStr("main"))
}

func TestAttachJobAttributes(t *testing.T) {
	// arrange
	job := Job{
		ID:              1,
		Name:            "Job Name",
		Status:          "complete",
		Conclusion:      "success",
		StartedAt:       time.Now(),
		CompletedAt:     time.Now(),
		URL:             "https://example.com",
		RunID:           1,
		RunnerName:      "Runner Name",
		RunnerGroupID:   1,
		RunnerGroupName: "Runner Group Name",
	}
	logRecord := plog.NewLogRecord()

	// act
	attachJobAttributes(&logRecord, job)

	// assert
	assert.Equal(t, 6, logRecord.Attributes().Len())
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.id", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.name", pcommon.NewValueStr("Job Name"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.started_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(job.StartedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.completed_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(job.CompletedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.conclusion", pcommon.NewValueStr("success"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.status", pcommon.NewValueStr("complete"))
}

func TestAttachStepAttributes(t *testing.T) {
	// arrange
	step := Step{
		Name:        "Step Name",
		Status:      "complete",
		StartedAt:   time.Now(),
		CompletedAt: time.Now(),
		Conclusion:  "success",
		Number:      1,
	}
	logRecord := plog.NewLogRecord()

	// act
	attachStepAttributes(&logRecord, step)

	// assert
	assert.Equal(t, 6, logRecord.Attributes().Len())
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.step.name", pcommon.NewValueStr("Step Name"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.step.number", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.step.started_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(step.StartedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.step.completed_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(step.CompletedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.step.conclusion", pcommon.NewValueStr("success"))
}

func TestParseLogLine(t *testing.T) {
	// arrange
	line := "2021-10-01T00:00:00Z Some message"

	// act
	logLine, err := parseLogLine(line)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, "Some message", logLine.Body)
	assert.Equal(t, 0, logLine.SeverityNumber)
}

func TestParseLogLineDebug(t *testing.T) {
	// arrange
	line := "2021-10-01T00:00:00Z ##[debug] debug message"

	// act
	logLine, err := parseLogLine(line)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, "##[debug] debug message", logLine.Body)
	assert.Equal(t, 5, logLine.SeverityNumber)
	assert.Equal(t, "DEBUG", logLine.SeverityText)
}

func TestParseLogLineError(t *testing.T) {
	// arrange
	line := "2021-10-01T00:00:00Z ##[error] error message"

	// act
	logLine, err := parseLogLine(line)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, "##[error] error message", logLine.Body)
	assert.Equal(t, 17, logLine.SeverityNumber)
	assert.Equal(t, "ERROR", logLine.SeverityText)
}

func TestParseLogErr(t *testing.T) {
	// arrange
	line := "2021-10-00Z Some message"

	// act
	_, err := parseLogLine(line)

	// assert
	assert.ErrorContains(t, err, "parsing time \"2021-10-00Z\" as \"2006-01-02T15:04:05.999999999Z07:00\": cannot parse \"Z\" as \"T\"")
}
