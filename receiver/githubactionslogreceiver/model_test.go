package githubactionslogreceiver

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStepsSort(t *testing.T) {
	steps := Steps{
		{
			Number: 2,
		},
		{
			Number: 1,
		},
		{
			Number: 3,
		},
	}
	steps.Sort()

	assert.Equal(t, int64(1), steps[0].Number)
	assert.Equal(t, int64(2), steps[1].Number)
	assert.Equal(t, int64(3), steps[2].Number)
}
