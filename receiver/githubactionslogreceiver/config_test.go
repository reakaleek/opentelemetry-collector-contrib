package githubactionslogreceiver_test

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubactionslogreceiver"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfigValidateSuccess(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path: "/test",
		GitHubAuth: githubactionslogreceiver.GitHubAuth{
			Token: "token",
		},
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
	assert.Equal(t, "either github_auth.token or github_auth.app_id must be set", err.Error())
}

func TestConfigValidateMalformedPathShouldFail(t *testing.T) {
	// arrange
	config := &githubactionslogreceiver.Config{
		Path: "lol !",
		GitHubAuth: githubactionslogreceiver.GitHubAuth{
			Token: "fake-token",
		},
	}

	// act
	err := config.Validate()

	// assert
	assert.EqualError(t, err, "path must be a valid URL: parse \"lol !\": invalid URI for request")
}

func TestConfigValidateAbsolutePathShouldFail(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		Path: "https://www.example.com/events",
		GitHubAuth: githubactionslogreceiver.GitHubAuth{
			Token: "fake-token",
		},
	}
	err := config.Validate()
	assert.EqualError(t, err, "path must be a relative URL. e.g. \"/events\"")
}

func TestConfigValidateNoAuthShouldFail(t *testing.T) {
	config := &githubactionslogreceiver.Config{}
	err := config.Validate()
	assert.EqualError(t, err, "either github_auth.token or github_auth.app_id must be set")
}

func TestConfigValidateGitHubAppShouldSucceed(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		GitHubAuth: githubactionslogreceiver.GitHubAuth{
			AppID:          123,
			InstallationID: 456,
			PrivateKey:     "fake",
		},
	}
	err := config.Validate()

	assert.NoError(t, err)
}

func TestConfigValidateGitHubAppPrivateKeyPathShouldSucceed(t *testing.T) {
	config := &githubactionslogreceiver.Config{
		GitHubAuth: githubactionslogreceiver.GitHubAuth{
			AppID:          123,
			InstallationID: 456,
			PrivateKeyPath: "fake",
		},
	}
	err := config.Validate()

	assert.NoError(t, err)
}

func TestConfigValidateOnlyAppIdShouldFail(t *testing.T) {
	// arrange
	config := &githubactionslogreceiver.Config{
		GitHubAuth: githubactionslogreceiver.GitHubAuth{
			AppID: 123,
		},
	}

	// act
	err := config.Validate()

	// assert
	assert.EqualError(t, err, "github_auth.installation_id must be set if github_auth.app_id is set; either github_auth.private_key or github_auth.private_key_path must be set if github_auth.app_id is set")
}
