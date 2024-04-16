package githubactionslogreceiver

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"github.com/google/go-github/v60/github"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHealthCheckHandler(t *testing.T) {
	ghalr := githubActionsLogReceiver{}
	assert.HTTPSuccess(
		t,
		func(writer http.ResponseWriter, request *http.Request) {
			ghalr.handleHealthCheck(writer, request, nil)
		},
		"GET",
		"/health",
		url.Values{},
	)
}

func TestWorkflowRunHandlerCompletedAction(t *testing.T) {
	defer gock.Off()

	// arrange
	const logURL = "https://example-log-url.com"
	jsonData, err := os.ReadFile("./testdata/fixtures/workflow_jobs.response.json")
	if err != nil {
		t.Fatal(err)
	}
	gock.
		New("https://api.github.com/repos/unelastisch/test-workflow-runs/actions/runs/8436609886/attempts/1/jobs?per_page=100").
		Reply(200).
		BodyString(string(jsonData))
	gock.
		New("https://api.github.com/repos/unelastisch/test-workflow-runs/actions/runs/8436609886/logs").
		Reply(http.StatusFound).
		AddHeader("Location", logURL)
	logFileNames := []string{
		"1_Set up job.txt",
		"2_Run actions_checkout@v2.txt",
		"3_Set up Ruby.txt",
		"4_Run actions_cache@v3.txt",
		"5_Install Bundler.txt",
		"6_Install Gems.txt",
		"7_Run Tests.txt",
		"8_Deploy to Heroku.txt",
		"16_Post actions_cache@v3.txt",
		"17_Complete job.txt",
	}
	buf := new(bytes.Buffer)
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		for _, logFileName := range logFileNames {
			file, err := writer.Create(filepath.Join("build", logFileName))
			if err != nil {
				t.Fatal(err)
			}
			timestamp := time.Now().Format("2006-01-02T15:04:05Z")

			_, err = file.Write([]byte(fmt.Sprintf("%s Logs of %s", timestamp, logFileName)))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	reader := bytes.NewReader(buf.Bytes())
	gock.
		New(logURL).
		Reply(200).
		Body(reader)
	ghClient := github.NewClient(nil)
	consumer := new(consumertest.LogsSink)
	ghalr := githubActionsLogReceiver{
		logger: zaptest.NewLogger(t),
		config: &Config{
			GitHubAuth: GitHubAuth{
				Token: "token",
			},
		},
		runLogCache: rlc{},
		consumer:    consumer,
		ghClient:    ghClient,
	}
	workflowRunJsonData, err := os.ReadFile("./testdata/fixtures/workflow_run-completed.event.json")
	if err != nil {
		t.Fatal(err)
	}
	workflowRunReader := bytes.NewReader(workflowRunJsonData)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", workflowRunReader)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-GitHub-Event", "workflow_run")
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ghalr.handleEvent(w, r, nil)
	})

	// act
	handler.ServeHTTP(w, r)

	// assert
	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, gock.IsDone())
	assert.Len(t, logFileNames, consumer.LogRecordCount())
	assert.Len(t, consumer.AllLogs(), 1)
	for i := 0; i < len(logFileNames); i++ {
		assert.Equal(
			t,
			fmt.Sprintf("Logs of %s", logFileNames[i]),
			consumer.AllLogs()[0].
				ResourceLogs().
				At(0).
				ScopeLogs().
				At(0).
				LogRecords().
				At(i).
				Body().
				Str(),
		)
	}
	attributesLen := consumer.AllLogs()[0].
		ResourceLogs().
		At(0).
		ScopeLogs().
		At(0).
		LogRecords().
		At(0).
		Attributes().Len()
	assert.Equal(t, 27, attributesLen)
}

func TestWorkflowRunHandlerRequestedAction(t *testing.T) {
	// arrange
	ghalr := githubActionsLogReceiver{
		logger: zaptest.NewLogger(t),
		config: &Config{
			GitHubAuth: GitHubAuth{
				Token: "token",
			},
		},
		runLogCache: rlc{},
		consumer:    consumertest.NewNop(),
	}
	workflowRunJsonData := []byte(`{ "action": "requested" }`)
	workflowRunReader := bytes.NewReader(workflowRunJsonData)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", workflowRunReader)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-GitHub-Event", "workflow_run")
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ghalr.handleEvent(w, r, nil)
	})

	// act
	handler.ServeHTTP(w, r)

	// assert
	assert.Equal(t, http.StatusOK, w.Code)
}

type failingConsumer struct {
	consumertest.Consumer
	consumeLogsFunc func(context.Context, plog.Logs) error
}

func (fc *failingConsumer) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	return fc.consumeLogsFunc(ctx, logs)
}

func TestConsumeLogsWithRetry(t *testing.T) {
	// arrange
	retryCounter := 0
	consumer := &failingConsumer{
		consumeLogsFunc: func(context.Context, plog.Logs) error {
			retryCounter++
			if retryCounter == 5 {
				return nil
			}
			return consumererror.NewLogs(fmt.Errorf("error %d", retryCounter), plog.NewLogs())
		},
	}
	ghalr := newLogsReceiver(
		&Config{
			GitHubAuth: GitHubAuth{
				Token: "token",
			},
			Path:            defaultPath,
			HealthCheckPath: defaultHealthCheckPath,
		},
		receivertest.NewNopCreateSettings(),
		consumer,
	)

	// act
	err := ghalr.consumeLogsWithRetry(
		context.Background(),
		func(fields ...zap.Field) []zap.Field {
			return make([]zap.Field, 0)
		},
		plog.NewLogs(),
	)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, 5, retryCounter)
}

func TestConsumeLogsWithRetryPermanent(t *testing.T) {
	// arrange
	retryCounter := 0
	consumer := &failingConsumer{
		consumeLogsFunc: func(context.Context, plog.Logs) error {
			retryCounter++
			if retryCounter == 5 {
				return consumererror.NewPermanent(fmt.Errorf("permanent error"))
			}
			return fmt.Errorf("error %d", retryCounter)
		},
	}
	ghalr := newLogsReceiver(
		&Config{
			GitHubAuth: GitHubAuth{
				Token: "token",
			},
			Path:            defaultPath,
			HealthCheckPath: defaultHealthCheckPath,
		},
		receivertest.NewNopCreateSettings(),
		consumer,
	)

	// act
	err := ghalr.consumeLogsWithRetry(
		context.Background(),
		func(fields ...zap.Field) []zap.Field {
			return make([]zap.Field, 0)
		},
		plog.NewLogs(),
	)

	// assert
	assert.Error(t, err)
	assert.Equal(t, 5, retryCounter)
}

func TestConsumeLogsWithRetryMaxElapsedTime(t *testing.T) {
	// arrange
	retryCounter := 0
	consumer := &failingConsumer{
		consumeLogsFunc: func(context.Context, plog.Logs) error {
			time.Sleep(1 * time.Millisecond)
			retryCounter++
			if retryCounter == 5 {
				return nil
			}
			return consumererror.NewLogs(fmt.Errorf("error %d", retryCounter), plog.NewLogs())
		},
	}
	ghalr := newLogsReceiver(
		&Config{
			GitHubAuth: GitHubAuth{
				Token: "token",
			},
			Path:            defaultPath,
			HealthCheckPath: defaultHealthCheckPath,
			Retry: RetryConfig{
				MaxElapsedTime: 2 * time.Millisecond,
			},
		},
		receivertest.NewNopCreateSettings(),
		consumer,
	)

	// act
	err := ghalr.consumeLogsWithRetry(
		context.Background(),
		func(fields ...zap.Field) []zap.Field {
			return make([]zap.Field, 0)
		},
		plog.NewLogs(),
	)

	// assert
	assert.Error(t, err)
	assert.Equal(t, 2, retryCounter)
}
