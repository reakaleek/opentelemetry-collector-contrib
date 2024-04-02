package githubactionslogreceiver

import (
	"fmt"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/multierr"
	"net/url"
)

const (
	defaultPort            = 19419
	defaultPath            = "/events"
	defaultHealthCheckPath = "/health"
)

type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`
	Path                    string              `mapstructure:"path"`
	HealthCheckPath         string              `mapstructure:"health_check_path"`
	WebhookSecret           configopaque.String `mapstructure:"webhook_secret"`
	GitHubAuth              GitHubAuth          `mapstructure:"github_auth"`
}

type GitHubAuth struct {
	AppID          int64               `mapstructure:"app_id"`
	InstallationID int64               `mapstructure:"installation_id"`
	PrivateKey     configopaque.String `mapstructure:"private_key"`
	PrivateKeyPath string              `mapstructure:"private_key_path"`

	Token configopaque.String `mapstructure:"token"`
}

// Validate checks if the receiver configuration is valid
func (cfg *Config) Validate() error {
	var err error
	if cfg.Path != "" {
		parsedUrl, parseErr := url.ParseRequestURI(cfg.Path)
		if parseErr != nil {
			err = multierr.Append(err, fmt.Errorf("path must be a valid URL: %s", parseErr))
		}
		if parsedUrl != nil && parsedUrl.Host != "" {
			err = multierr.Append(err, fmt.Errorf("path must be a relative URL. e.g. \"/events\""))
		}
	}
	if cfg.GitHubAuth.Token == "" && cfg.GitHubAuth.AppID == 0 {
		err = multierr.Append(err, fmt.Errorf("either auth.token or auth.app_id must be set"))
	}
	if cfg.GitHubAuth.AppID != 0 {
		if cfg.GitHubAuth.InstallationID == 0 {
			err = multierr.Append(err, fmt.Errorf("auth.installation_id must be set if auth.app_id is set"))
		}
		if cfg.GitHubAuth.PrivateKey == "" && cfg.GitHubAuth.PrivateKeyPath == "" {
			err = multierr.Append(err, fmt.Errorf("either auth.private_key or auth.private_key_path must be set if auth.app_id is set"))
		}
	}
	return err
}
