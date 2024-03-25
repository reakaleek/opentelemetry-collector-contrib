package githubactionslogreceiver_test

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubactionslogreceiver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Config_Validate_Success(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path:        "/test",
		GitHubToken: "fake_token",
	}
	err := config.Validate()
	assert.NoError(t, err)
}

func Test_Config_Validate_Missing_GitHubToken_Should_Fail(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path: "/test",
	}
	err := config.Validate()
	assert.Error(t, err)
}

func Test_Config_Validate_Malformed_Path_Should_Fail(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path:        "lol !",
		GitHubToken: "fake_token",
	}
	err := config.Validate()
	assert.ErrorContains(t, err, "path must be a valid URL")
}
