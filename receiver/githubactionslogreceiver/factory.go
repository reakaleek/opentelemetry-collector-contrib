package githubactionslogreceiver

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/localhostgate"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/githubactionslogreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

func createDefaultConfig() component.Config {
	return &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: localhostgate.EndpointForPort(defaultPort),
		},
		Path:            defaultPath,
		HealthCheckPath: defaultHealthCheckPath,
		Retry: RetryConfig{
			InitialInterval: defaultRetryInitialInterval,
			MaxInterval:     defaultRetryMaxInterval,
			MaxElapsedTime:  defaultRetryMaxElapsedTime,
		},
		BatchSize: 10000,
	}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.CreateSettings,
	rConf component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	cfg := rConf.(*Config)
	return newLogsReceiver(cfg, params, consumer)
}

// NewFactory creates a factory for githubactionslogsreceiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(metadata.Type.String()),
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}
