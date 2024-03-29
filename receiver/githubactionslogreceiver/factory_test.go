package githubactionslogreceiver

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"testing"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	_, err := factory.CreateLogsReceiver(
		context.Background(),
		receivertest.NewNopCreateSettings(),
		cfg,
		consumertest.NewNop(),
	)
	assert.NoError(t, err)
}
