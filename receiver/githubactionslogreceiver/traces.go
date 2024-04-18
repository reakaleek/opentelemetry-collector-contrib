package githubactionslogreceiver

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"strings"
)

// generateTraceID generates a trace ID from the run ID and run attempt
// Copied from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/b45f1739c29039a0314b9c97c3ccc578562df36f/receiver/githubactionsreceiver/trace_event_handling.go#L212
// to be able to correlate logs with traces from the githubactionsreceiver
func generateTraceID(runID int64, runAttempt int) (pcommon.TraceID, error) {
	input := fmt.Sprintf("%d%dt", runID, runAttempt)
	hash := sha256.Sum256([]byte(input))
	traceIDHex := hex.EncodeToString(hash[:])

	var traceID pcommon.TraceID
	_, err := hex.Decode(traceID[:], []byte(traceIDHex[:32]))
	if err != nil {
		return pcommon.TraceID{}, err
	}

	return traceID, nil
}

// generateStepSpanID generates a span ID from the run ID, run attempt, job name, step name and step number
// Copied from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/b45f1739c29039a0314b9c97c3ccc578562df36f/receiver/githubactionsreceiver/trace_event_handling.go#L262
// to be able to correlate logs with traces from the githubactionsreceiver
func generateStepSpanID(runID int64, runAttempt int, jobName, stepName string, stepNumber ...int64) (pcommon.SpanID, error) {
	var input string
	if len(stepNumber) > 0 && stepNumber[0] > 0 {
		input = fmt.Sprintf("%d%d%s%s%d", runID, runAttempt, jobName, stepName, stepNumber[0])
	} else {
		input = fmt.Sprintf("%d%d%s%s", runID, runAttempt, jobName, stepName)
	}
	hash := sha256.Sum256([]byte(input))
	spanIDHex := hex.EncodeToString(hash[:])

	var spanID pcommon.SpanID
	_, err := hex.Decode(spanID[:], []byte(spanIDHex[16:32]))
	if err != nil {
		return pcommon.SpanID{}, err
	}
	return spanID, nil
}

// generateServiceName generates a service name from the full name of the repository
// Copied from https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/b45f1739c29039a0314b9c97c3ccc578562df36f/receiver/githubactionsreceiver/trace_event_handling.go#L254
// to be able to correlate logs with traces from the githubactionsreceiver
func generateServiceName(config *Config, fullName string) string {
	if config.CustomServiceName != "" {
		return config.CustomServiceName
	}
	formattedName := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(fullName, "/", "-"), "_", "-"))
	return fmt.Sprintf("%s%s%s", config.ServiceNamePrefix, formattedName, config.ServiceNameSuffix)
}
