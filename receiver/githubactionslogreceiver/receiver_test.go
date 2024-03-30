package githubactionslogreceiver

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
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
		New("https://api.github.com/repos/unelastisch/test-workflow-runs/actions/runs/8436609886/jobs").
		Reply(200).
		BodyString(string(jsonData))
	gock.
		New("https://api.github.com/repos/unelastisch/test-workflow-runs/actions/runs/8436609886/logs").
		Reply(http.StatusFound).
		AddHeader("Location", logURL)
	gock.
		New("https://api.github.com/rate_limit").
		Reply(200).
		BodyString(`{"resources":{"core":{"limit":5000,"remaining":4999,"reset":1631539200},"search":{"limit":30,"remaining":30,"reset":1631539200},"graphql":{"limit":5000,"remaining":5000,"reset":1631539200},"integration_manifest":{"limit":5000,"remaining":5000,"reset":1631539200}}}`)

	zipFile, err := os.ReadFile("./testdata/fixtures/logs.zip")
	zipFileReader := bytes.NewReader(zipFile)
	gock.
		New(logURL).
		Reply(200).
		Body(zipFileReader)
	ghalr := githubActionsLogReceiver{
		logger: zaptest.NewLogger(t),
		config: &Config{
			GitHubAuth: Auth{
				Token: "token",
			},
		},
		runLogCache: rlc{},
		consumer:    consumertest.NewNop(),
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
		ghalr.handleWorkflowRun(w, r, nil)
	})

	// act
	handler.ServeHTTP(w, r)

	// assert
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestWorkflowRunHandlerRequestedAction(t *testing.T) {
	// arrange
	ghalr := githubActionsLogReceiver{
		logger: zaptest.NewLogger(t),
		config: &Config{
			GitHubAuth: Auth{
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
		ghalr.handleWorkflowRun(w, r, nil)
	})

	// act
	handler.ServeHTTP(w, r)

	// assert
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestStartAndShutDown(t *testing.T) {
	ghalr := githubActionsLogReceiver{
		logger: zaptest.NewLogger(t),
		config: &Config{
			GitHubAuth: Auth{
				Token: "token",
			},
			Path:            defaultPath,
			HealthCheckPath: defaultHealthCheckPath,
		},
		settings: receivertest.NewNopCreateSettings(),
	}
	err := ghalr.Shutdown(nil)
	assert.NoError(t, err)
	err = ghalr.Start(nil, nil)
	assert.NoError(t, err)
	err = ghalr.Shutdown(nil)
	assert.NoError(t, err)
}

func TestCreateGitHubClient(t *testing.T) {
	// arrange
	ghalr := githubActionsLogReceiver{
		config: &Config{
			GitHubAuth: Auth{
				Token: "token",
			},
		},
	}

	// act
	_, err := createGitHubClient(&ghalr)

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
	ghalr := githubActionsLogReceiver{
		config: &Config{
			GitHubAuth: Auth{
				AppID:          123,
				InstallationID: 123,
				PrivateKeyPath: fp,
			},
		},
	}

	// act
	_, err = createGitHubClient(&ghalr)

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
	ghalr := githubActionsLogReceiver{
		config: &Config{
			GitHubAuth: Auth{
				AppID:          123,
				InstallationID: 123,
				PrivateKey:     configopaque.String(encodedPrivateKey),
			},
		},
	}

	// act
	_, err = createGitHubClient(&ghalr)

	// assert
	assert.NoError(t, err)
}

func TestCreateGitHubClient4(t *testing.T) {
	// arrange
	ghalr := githubActionsLogReceiver{
		config: &Config{
			GitHubAuth: Auth{
				AppID:          123,
				InstallationID: 123,
				PrivateKey:     "malformed private key",
			},
		},
	}

	// act
	_, err := createGitHubClient(&ghalr)

	// assert
	assert.EqualError(t, err, "could not parse private key: invalid key: Key must be a PEM encoded PKCS1 or PKCS8 key")
}
