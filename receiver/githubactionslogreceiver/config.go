package githubactionslogreceiver

import (
	"fmt"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.uber.org/multierr"
	"net/url"
	"time"
)

const (
	defaultPort                 = 19419
	defaultPath                 = "/events"
	defaultHealthCheckPath      = "/health"
	defaultRetryInitialInterval = 1 * time.Second
	defaultRetryMaxInterval     = 30 * time.Minute
	defaultRetryMaxElapsedTime  = 5 * time.Minute
)

type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`
	Path                    string              `mapstructure:"path"`
	HealthCheckPath         string              `mapstructure:"health_check_path"`
	WebhookSecret           configopaque.String `mapstructure:"webhook_secret"`
	GitHubAuth              GitHubAuth          `mapstructure:"github_auth"`
	Retry                   RetryConfig         `mapstructure:"retry"`
	BatchSize               int                 `mapstructure:"batch_size"`
	CustomServiceName       string              `mapstructure:"custom_service_name"`
	ServiceNamePrefix       string              `mapstructure:"service_name_prefix"`
	ServiceNameSuffix       string              `mapstructure:"service_name_suffix"`
}

type RetryConfig struct {
	InitialInterval time.Duration `mapstructure:"initial_interval"`
	MaxInterval     time.Duration `mapstructure:"max_interval"`
	MaxElapsedTime  time.Duration `mapstructure:"max_elapsed_time"`
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
		err = multierr.Append(err, fmt.Errorf("either github_auth.token or github_auth.app_id must be set"))
	}
	if cfg.GitHubAuth.AppID != 0 {
		if cfg.GitHubAuth.InstallationID == 0 {
			err = multierr.Append(err, fmt.Errorf("github_auth.installation_id must be set if github_auth.app_id is set"))
		}
		if cfg.GitHubAuth.PrivateKey == "" && cfg.GitHubAuth.PrivateKeyPath == "" {
			err = multierr.Append(err, fmt.Errorf("either github_auth.private_key or github_auth.private_key_path must be set if github_auth.app_id is set"))
		}
	}
	return err
}
