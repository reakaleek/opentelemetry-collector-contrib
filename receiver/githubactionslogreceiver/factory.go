package githubactionslogreceiver

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/localhostgate"
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
	}
}

func createLogsReceiver(
	_ context.Context,
	params receiver.CreateSettings,
	rConf component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	cfg := rConf.(*Config)
	return newLogsReceiver(cfg, params, consumer), nil
}

// NewFactory creates a factory for githubactionslogsreceiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType("githubactionslog"),
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelDevelopment),
	)
}