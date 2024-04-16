package githubactionslogreceiver

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/configopaque"
	"os"
	"path/filepath"
	"testing"
)

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
