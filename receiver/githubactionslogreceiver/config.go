package githubactionslogreceiver

import (
	"fmt"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"net/url"
)

const (
	defaultPort            = 19418
	defaultPath            = "/workflow-run-events"
	defaultHealthCheckPath = "/health"
)

type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`
	Path                    string              `mapstructure:"path"`
	HealthCheckPath         string              `mapstructure:"health_check_path"`
	GitHubToken             configopaque.String `mapstructure:"github_token"`
}

// Validate checks if the receiver configuration is valid
func (cfg *Config) Validate() error {
	_, err := url.ParseRequestURI(cfg.Path)
	if err != nil {
		return fmt.Errorf("path must be a valid URL: %s", err)
	}
	if cfg.GitHubToken == "" {
		return fmt.Errorf("github_token must be set. See https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens for more information on how to create a token")
	}
	return nil
}
