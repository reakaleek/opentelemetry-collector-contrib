package githubactionslogreceiver_test

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubactionslogreceiver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfigValidateSuccess(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path:        "/test",
		GitHubToken: "faketoken",
	}
	err := config.Validate()
	assert.NoError(t, err)
}

func TestConfigValidateMissingGitHubTokenShouldFail(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path: "/test",
	}
	err := config.Validate()
	assert.Error(t, err)
}

func TestConfigValidateMalformedPathShouldFail(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path:        "lol !",
		GitHubToken: "faketoken",
	}
	err := config.Validate()
	assert.ErrorContains(t, err, "path must be a valid URL")
}
