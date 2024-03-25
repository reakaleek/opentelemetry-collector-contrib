package githubactionslogreceiver

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestRunLogCache_Exists_Should_Return_True(t *testing.T) {
	filename := fmt.Sprintf("run-log-%d-%d.zip", 1337, 42)
	fp := filepath.Join(t.TempDir(), "run-log-cache", filename)
	cache := rlc{}
	respReader := bytes.NewReader([]byte("test"))
	err := cache.Create(fp, respReader)
	if err != nil {
		t.Error(err)
	}
	assert.True(t, cache.Exists(fp))
}

func TestRunLogCache_Exists_Should_Return_False(t *testing.T) {
	cache := rlc{}
	filename := fmt.Sprintf("run-log-%d-%d.zip", 1337, 42)
	fp := filepath.Join(t.TempDir(), "run-log-cache", filename)
	assert.False(t, cache.Exists(fp))
}
