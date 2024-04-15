package githubactionslogreceiver

import (
	"archive/zip"
	"bytes"
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

func TestAttachRunAttributes(t *testing.T) {
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
	}

	logRecord := plog.NewLogRecord()

	attachRunAttributes(&logRecord, run)
	assert.Equal(t, 13, logRecord.Attributes().Len())
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.id", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.name", pcommon.NewValueStr("Run Name"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.run_attempt", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.run_number", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.url", pcommon.NewValueStr("https://example.com/attempts/1"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.conclusion", pcommon.NewValueStr("success"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.status", pcommon.NewValueStr("complete"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.event", pcommon.NewValueStr("push"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.created_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(run.CreatedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.updated_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(run.UpdatedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.actor.login", pcommon.NewValueStr("reakaleek"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_run.actor.id", pcommon.NewValueInt(1))
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
	assert.Equal(t, 10, logRecord.Attributes().Len())
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.id", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.name", pcommon.NewValueStr("Job Name"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.url", pcommon.NewValueStr("https://example.com"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.started_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(job.StartedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.completed_at", pcommon.NewValueStr(pcommon.NewTimestampFromTime(job.CompletedAt).String()))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.conclusion", pcommon.NewValueStr("success"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.status", pcommon.NewValueStr("complete"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.runner.group_id", pcommon.NewValueInt(1))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.runner.group_name", pcommon.NewValueStr("Runner Group Name"))
	assertAttributeEquals(t, logRecord.Attributes(), "github.workflow_job.runner.name", pcommon.NewValueStr("Runner Name"))
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

func TestToLogs(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	line := "2021-10-01T00:00:00Z Some message"
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create("file.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
	}()
	reader := bytes.NewReader(buf.Bytes())
	zipReader, err := zip.NewReader(reader, int64(len(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []Job{
		{
			Steps: Steps{
				{
					Log: zipReader.File[0],
				},
			},
		},
	}

	// act
	logs, err := toLogs(&githubActionsLogReceiver{}, Repository{}, Run{}, jobs)

	// assert
	logRecords := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	logLine, err := parseLogLine(line)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, logs.LogRecordCount())
	assert.Equal(t, pcommon.NewValueStr(logLine.Body), logRecords.At(0).Body())
	assert.Equal(t, pcommon.NewTimestampFromTime(logLine.Timestamp), logRecords.At(0).Timestamp())
	assert.NoError(t, err)
}

func TestToLogsMultipleLogLines(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	content := `2021-10-01T00:00:00Z Some message
2021-10-01T00:00:01Z Another message
2021-10-01T00:00:02Z Yet another message`
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create("file.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}()
	reader := bytes.NewReader(buf.Bytes())
	zipReader, err := zip.NewReader(reader, int64(len(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []Job{
		{
			Steps: Steps{
				{
					Log: zipReader.File[0],
				},
			},
		},
	}

	// act
	logs, err := toLogs(&githubActionsLogReceiver{}, Repository{}, Run{}, jobs)

	// assert
	logRecords := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, logs.LogRecordCount())
	assert.Equal(t, "Some message", logRecords.At(0).Body().Str())
	assert.Equal(t, "Another message", logRecords.At(1).Body().Str())
	assert.Equal(t, "Yet another message", logRecords.At(2).Body().Str())
	assert.NoError(t, err)
}

func TestToLogsMultineLogWithEmptyLine(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	content := `2021-10-01T00:00:00Z Some message
2021-10-01T00:00:01Z Another message

2021-10-01T00:00:02Z Yet another message
`
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create("file.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}()
	reader := bytes.NewReader(buf.Bytes())
	zipReader, err := zip.NewReader(reader, int64(len(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []Job{
		{
			Steps: Steps{
				{
					Log: zipReader.File[0],
				},
			},
		},
	}

	// act
	logs, err := toLogs(&githubActionsLogReceiver{}, Repository{}, Run{}, jobs)

	// assert
	logRecords := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, logs.LogRecordCount())
	assert.Equal(t, "Some message", logRecords.At(0).Body().Str())
	assert.Equal(t, "Another message", logRecords.At(1).Body().Str())
	assert.Equal(t, "Yet another message", logRecords.At(2).Body().Str())
	assert.NoError(t, err)
}

func TestToLogsMultLineLog(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	content := `2021-10-01T00:00:00Z Some message
2021-10-01T00:00:01Z Another message
Gibberish
Foo Bar

2021-10-01T00:00:02Z Yet another message
`
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create("file.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}()
	reader := bytes.NewReader(buf.Bytes())
	zipReader, err := zip.NewReader(reader, int64(len(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []Job{
		{
			Steps: Steps{
				{
					Log: zipReader.File[0],
				},
			},
		},
	}

	// act
	logs, err := toLogs(&githubActionsLogReceiver{}, Repository{}, Run{}, jobs)

	// assert
	logRecords := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, logs.LogRecordCount())
	assert.Equal(t, "Some message", logRecords.At(0).Body().Str())
	assert.Equal(t, "Another message\nGibberish\nFoo Bar", logRecords.At(1).Body().Str())
	assert.Equal(t, "Yet another message", logRecords.At(2).Body().Str())
	assert.NoError(t, err)
}

func TestToLogsStartingWithEmptyLines(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	content := `


2021-10-01T00:00:00Z Some message
2021-10-01T00:00:01Z Another message
2021-10-01T00:00:02Z Yet another message
`
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create("file.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(content))
		if err != nil {
			t.Fatal(err)
		}
	}()
	reader := bytes.NewReader(buf.Bytes())
	zipReader, err := zip.NewReader(reader, int64(len(buf.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	jobs := []Job{
		{
			Steps: Steps{
				{
					Log: zipReader.File[0],
				},
			},
		},
	}

	// act
	logs, err := toLogs(&githubActionsLogReceiver{}, Repository{}, Run{}, jobs)

	// assert
	logRecords := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, logs.LogRecordCount())
	assert.Equal(t, "Some message", logRecords.At(0).Body().Str())
	assert.Equal(t, "Another message", logRecords.At(1).Body().Str())
	assert.Equal(t, "Yet another message", logRecords.At(2).Body().Str())
	assert.NoError(t, err)
}
