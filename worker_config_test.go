package dynasc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithBatchSize(t *testing.T) {
	tests := []struct {
		name string
		in   int32
	}{
		{
			name: "Normal: a batch size is specified",
			in:   1,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := &Worker{}
			WithBatchSize(tc.in)(w)
			assert.Equal(t, tc.in, *w.batchSize)
		})
	}
}

func TestWithHooks(t *testing.T) {
	tests := []struct {
		name string
		in   *WorkerHooks
	}{
		{
			name: "Normal: hooks are specified",
			in: &WorkerHooks{
				PreSetup: func(ctx context.Context) error {
					return nil
				},
				PostSetup: func(ctx context.Context) error {
					return nil
				},
			},
		},
		{
			name: "Normal: hooks are not specified",
			in:   nil,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := &Worker{}
			WithHooks(tc.in)(w)
			assert.Equal(t, tc.in, w.hooks)
		})
	}
}
