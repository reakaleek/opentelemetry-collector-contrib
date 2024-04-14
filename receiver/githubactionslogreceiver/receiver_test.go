package githubactionslogreceiver

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/google/go-github/v60/github"
	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap/zaptest"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAttachRunLog(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create(filepath.Join("SomeJob", "1_stepname.txt"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(""))
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
			Name: "SomeJob",
			Steps: Steps{
				{
					Number: 1,
				},
			},
		},
	}

	// act
	attachRunLog(zipReader, jobs)
	// assert
	assert.NotNil(t, jobs[0].Steps[0].Log)
}

func TestAttachRunLog2(t *testing.T) {
	// arrange
	buf := new(bytes.Buffer)
	func() {
		writer := zip.NewWriter(buf)
		defer writer.Close()
		file, err := writer.Create(filepath.Join("job  action", "1_stepname.txt"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = file.Write([]byte(""))
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
			Name: "job / action",
			Steps: Steps{
				{
					Number: 1,
				},
			},
		},
	}

	// act
	attachRunLog(zipReader, jobs)

	// assert
	assert.NotNil(t, jobs[0].Steps[0].Log)
}

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
		eventQueue:  make(chan *github.WorkflowRunEvent, 100),
	}
	go ghalr.processEvents()

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
	close(ghalr.eventQueue)
	ghalr.wg.Wait()

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
	go ghalr.processEvents()
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
	close(ghalr.eventQueue)
	ghalr.wg.Wait()

	// assert
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestStartAndShutDown(t *testing.T) {
	ghalr := newLogsReceiver(&Config{
		GitHubAuth: GitHubAuth{
			Token: "token",
		},
		Path:            defaultPath,
		HealthCheckPath: defaultHealthCheckPath,
	}, receivertest.NewNopCreateSettings(), consumertest.NewNop())
	err := ghalr.Start(nil, nil)
	assert.NoError(t, err)
	err = ghalr.Shutdown(nil)
	assert.NoError(t, err)
}

func TestCreateGitHubClient(t *testing.T) {
	// arrange
	ghAuth := GitHubAuth{
		Token: "token",
	}

	// act
	_, err := createGitHubClient(ghAuth)

	// assert
	assert.NoError(t, err)
}

func TestCreateGitHubClient2(t *testing.T) {
	// arrange private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return
	}
	pkcs1PrivateKey := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: pkcs1PrivateKey,
	}
	fp := filepath.Join(os.TempDir(), "private.*.pem")
	file, err := os.Create(fp)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := pem.Encode(file, privateKeyBlock); err != nil {
		t.Fatal(err)
	}

	// arrange receiver config
	ghAuth := GitHubAuth{
		AppID:          123,
		InstallationID: 123,
		PrivateKeyPath: fp,
	}

	// act
	_, err = createGitHubClient(ghAuth)

	// assert
	assert.NoError(t, err)
}

func TestCreateGitHubClient3(t *testing.T) {
	// arrange private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return
	}
	pkcs1PrivateKey := x509.MarshalPKCS1PrivateKey(privateKey)
	privateKeyBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: pkcs1PrivateKey,
	}
	encodedPrivateKey := pem.EncodeToMemory(privateKeyBlock)

	// arrange receiver config
	ghAuth := GitHubAuth{
		AppID:          123,
		InstallationID: 123,
		PrivateKey:     configopaque.String(encodedPrivateKey),
	}

	// act
	_, err = createGitHubClient(ghAuth)

	// assert
	assert.NoError(t, err)
}

func TestCreateGitHubClient4(t *testing.T) {
	// arrange
	ghAuth := GitHubAuth{
		AppID:          123,
		InstallationID: 123,
		PrivateKey:     "malformed private key",
	}

	// act
	_, err := createGitHubClient(ghAuth)

	// assert
	assert.EqualError(t, err, "could not parse private key: invalid key: Key must be a PEM encoded PKCS1 or PKCS8 key")
}
