package dynasc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DefaultWorkerHookManagerTestSuite struct {
	suite.Suite
}

func (s *DefaultWorkerHookManagerTestSuite) TestExecutePreSetup() {
	// Define structures for input and output.
	type in struct {
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name    string
		in      *in
		out     *out
		manager WorkerHookManager
	}{
		{
			name: "Normal: a pre setup function is specified",
			in:   &in{},
			out:  &out{},
			manager: NewWorkerHookManager(&WorkerHooks{
				PreSetup: func(ctx context.Context) error {
					return nil
				},
			}),
		},
		{
			name:    "Normal: a pre setup function is not specified",
			in:      &in{},
			out:     &out{},
			manager: NewWorkerHookManager(nil),
		},
		{
			name: "Abnormal: a pre setup function returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to execute a pre setup function"),
			},
			manager: NewWorkerHookManager(&WorkerHooks{
				PreSetup: func(ctx context.Context) error {
					return errors.New("")
				},
			}),
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := tc.manager.executePreSetup(ctx)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *DefaultWorkerHookManagerTestSuite) TestExecutePostSetup() {
	// Define structures for input and output.
	type in struct {
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name    string
		in      *in
		out     *out
		manager WorkerHookManager
	}{
		{
			name: "Normal: a post setup function is specified",
			in:   &in{},
			out:  &out{},
			manager: NewWorkerHookManager(&WorkerHooks{
				PostSetup: func(ctx context.Context) error {
					return nil
				},
			}),
		},
		{
			name:    "Normal: a post setup function is not specified",
			in:      &in{},
			out:     &out{},
			manager: NewWorkerHookManager(nil),
		},
		{
			name: "Abnormal: a post setup function returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to execute a post setup function"),
			},
			manager: NewWorkerHookManager(&WorkerHooks{
				PostSetup: func(ctx context.Context) error {
					return errors.New("")
				},
			}),
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := tc.manager.executePostSetup(ctx)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *DefaultWorkerHookManagerTestSuite) TestExecutePreStreamWorkerSetup() {
	// Define structures for input and output.
	type in struct {
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name    string
		in      *in
		out     *out
		manager WorkerHookManager
	}{
		{
			name: "Normal: a pre stream worker setup function is specified",
			in:   &in{},
			out:  &out{},
			manager: NewWorkerHookManager(&WorkerHooks{
				PreStreamWorkerSetup: func(ctx context.Context, streamARN string) error {
					return nil
				},
			}),
		},
		{
			name:    "Normal: a pre stream worker setup function is not specified",
			in:      &in{},
			out:     &out{},
			manager: NewWorkerHookManager(nil),
		},
		{
			name: "Abnormal: a pre stream worker setup function returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to execute a pre stream worker setup function"),
			},
			manager: NewWorkerHookManager(&WorkerHooks{
				PreStreamWorkerSetup: func(ctx context.Context, streamARN string) error {
					return errors.New("")
				},
			}),
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := tc.manager.executePreStreamWorkerSetup(ctx, "arn:aws:dynamodb:000000000000")
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *DefaultWorkerHookManagerTestSuite) TestExecutePostStreamWorkerSetup() {
	// Define structures for input and output.
	type in struct {
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name    string
		in      *in
		out     *out
		manager WorkerHookManager
	}{
		{
			name: "Normal: a post stream worker setup function is specified",
			in:   &in{},
			out:  &out{},
			manager: NewWorkerHookManager(&WorkerHooks{
				PostStreamWorkerSetup: func(ctx context.Context, streamARN string) error {
					return nil
				},
			}),
		},
		{
			name:    "Normal: a post stream worker setup function is not specified",
			in:      &in{},
			out:     &out{},
			manager: NewWorkerHookManager(nil),
		},
		{
			name: "Normal: a map of shard worker wait groups exists",
			in:   &in{},
			out:  &out{},
			manager: func() WorkerHookManager {
				m := NewWorkerHookManager(nil)
				m.shardWaitGroups.Store("arn:aws:dynamodb:000000000000", &TypedSyncMap[string, *sync.WaitGroup]{})
				return m
			}(),
		},
		{
			name: "Normal: a stream worker wait group exists",
			in:   &in{},
			out:  &out{},
			manager: func() WorkerHookManager {
				m := NewWorkerHookManager(nil)
				m.streamWaitGroups.Store("arn:aws:dynamodb:000000000000", func() *sync.WaitGroup {
					wg := &sync.WaitGroup{}
					wg.Add(1)
					return wg
				}())
				return m
			}(),
		},
		{
			name: "Abnormal: a post stream worker setup function returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to execute a post stream worker setup function"),
			},
			manager: NewWorkerHookManager(&WorkerHooks{
				PostStreamWorkerSetup: func(ctx context.Context, streamARN string) error {
					return errors.New("")
				},
			}),
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := tc.manager.executePostStreamWorkerSetup(ctx, "arn:aws:dynamodb:000000000000")
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *DefaultWorkerHookManagerTestSuite) TestExecutePreShardWorkerSetup() {
	// Define structures for input and output.
	type in struct {
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name    string
		in      *in
		out     *out
		manager WorkerHookManager
	}{
		{
			name: "Normal: a pre shard worker setup function is specified",
			in:   &in{},
			out:  &out{},
			manager: NewWorkerHookManager(&WorkerHooks{
				PreShardWorkerSetup: func(ctx context.Context, streamARN, shardID string) error {
					return nil
				},
			}),
		},
		{
			name:    "Normal: a pre shard worker setup function is not specified",
			in:      &in{},
			out:     &out{},
			manager: NewWorkerHookManager(nil),
		},
		{
			name: "Normal: a map of shard worker wait groups exists",
			in:   &in{},
			out:  &out{},
			manager: func() WorkerHookManager {
				m := NewWorkerHookManager(nil)
				m.shardWaitGroups.Store("arn:aws:dynamodb:000000000000", &TypedSyncMap[string, *sync.WaitGroup]{})
				return m
			}(),
		},
		{
			name: "Abnormal: a pre shard worker setup function returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to execute a pre shard worker setup function"),
			},
			manager: NewWorkerHookManager(&WorkerHooks{
				PreShardWorkerSetup: func(ctx context.Context, streamARN, shardID string) error {
					return errors.New("")
				},
			}),
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := tc.manager.executePreShardWorkerSetup(ctx, "arn:aws:dynamodb:000000000000", "shardId-00000000000000000000-00000000")
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *DefaultWorkerHookManagerTestSuite) TestExecutePostShardWorkerSetup() {
	// Define structures for input and output.
	type in struct {
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name    string
		in      *in
		out     *out
		manager WorkerHookManager
	}{
		{
			name: "Normal: a post shard worker setup function is specified",
			in:   &in{},
			out:  &out{},
			manager: NewWorkerHookManager(&WorkerHooks{
				PostShardWorkerSetup: func(ctx context.Context, streamARN, shardID string) error {
					return nil
				},
			}),
		},
		{
			name:    "Normal: a post shard worker setup function is not specified",
			in:      &in{},
			out:     &out{},
			manager: NewWorkerHookManager(nil),
		},
		{
			name: "Normal: a shard worker wait group exists",
			in:   &in{},
			out:  &out{},
			manager: func() WorkerHookManager {
				m := NewWorkerHookManager(nil)
				m.shardWaitGroups.Store("arn:aws:dynamodb:000000000000", func() *TypedSyncMap[string, *sync.WaitGroup] {
					wgs := &TypedSyncMap[string, *sync.WaitGroup]{}
					wgs.Store("shardId-00000000000000000000-00000000", func() *sync.WaitGroup {
						wg := &sync.WaitGroup{}
						wg.Add(1)
						return wg
					}())
					return wgs
				}())
				return m
			}(),
		},
		{
			name: "Abnormal: a post shard worker setup function returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to execute a post shard worker setup function"),
			},
			manager: NewWorkerHookManager(&WorkerHooks{
				PostShardWorkerSetup: func(ctx context.Context, streamARN, shardID string) error {
					return errors.New("")
				},
			}),
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := tc.manager.executePostShardWorkerSetup(ctx, "arn:aws:dynamodb:000000000000", "shardId-00000000000000000000-00000000")
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *DefaultWorkerHookManagerTestSuite) TestWait() {
	// Define structures for input and output.
	type in struct {
		wgs *TypedSyncMap[string, *sync.WaitGroup]
	}
	type out struct {
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: multiple wait groups exist",
			in: &in{
				wgs: func() *TypedSyncMap[string, *sync.WaitGroup] {
					wgs := &TypedSyncMap[string, *sync.WaitGroup]{}
					wgs.Store("key1", &sync.WaitGroup{})
					wgs.Store("key2", &sync.WaitGroup{})
					return wgs
				}(),
			},
			out: &out{},
		},
		{
			name: "Normal: no wait group exists",
			in: &in{
				wgs: &TypedSyncMap[string, *sync.WaitGroup]{},
			},
			out: &out{},
		},
	}
	// Execute.
	for i, tc := range tests {
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			NewWorkerHookManager(nil).wait(tc.in.wgs)
		})
	}
}

func TestDefaultWorkerHookManagerTestSuite(t *testing.T) {
	// Disable standard output as log output.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Execute.
	suite.Run(t, &DefaultWorkerHookManagerTestSuite{})
}

type NoopWorkerHookManagerTestSuite struct {
	suite.Suite
}

func (s *NoopWorkerHookManagerTestSuite) TestExecutePreSetup() {
	s.Nil(NewNoopWorkerHookManager().executePreSetup(context.Background()))
}

func (s *NoopWorkerHookManagerTestSuite) TestExecutePostSetup() {
	s.Nil(NewNoopWorkerHookManager().executePostSetup(context.Background()))
}

func (s *NoopWorkerHookManagerTestSuite) TestExecutePreStreamWorkerSetup() {
	s.Nil(NewNoopWorkerHookManager().executePreStreamWorkerSetup(context.Background(), "arn:aws:dynamodb:000000000000"))
}

func (s *NoopWorkerHookManagerTestSuite) TestExecutePostStreamWorkerSetup() {
	s.Nil(NewNoopWorkerHookManager().executePostStreamWorkerSetup(context.Background(), "arn:aws:dynamodb:000000000000"))
}

func (s *NoopWorkerHookManagerTestSuite) TestExecutePreShardWorkerSetup() {
	s.Nil(NewNoopWorkerHookManager().executePreShardWorkerSetup(context.Background(), "arn:aws:dynamodb:000000000000", "shardId-00000000000000000000-00000000"))
}

func (s *NoopWorkerHookManagerTestSuite) TestExecutePostShardWorkerSetup() {
	s.Nil(NewNoopWorkerHookManager().executePostShardWorkerSetup(context.Background(), "arn:aws:dynamodb:000000000000", "shardId-00000000000000000000-00000000"))
}

func TestNoopWorkerHookManagerTestSuite(t *testing.T) {
	// Disable standard output as log output.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Execute.
	suite.Run(t, &NoopWorkerHookManagerTestSuite{})
}

func TestWorkerHookManagerValue(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		ctx context.Context
	}
	type out struct {
		manager WorkerHookManager
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: a worker hook manager exists in context",
			in: &in{
				ctx: context.WithValue(context.Background(), workerHookManagerKey{}, NewWorkerHookManager(nil)),
			},
			out: &out{
				manager: NewWorkerHookManager(nil),
			},
		},
		{
			name: "Normal: a worker hook manager exists in context",
			in: &in{
				ctx: context.Background(),
			},
			out: &out{
				manager: NewNoopWorkerHookManager(),
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out.manager, WorkerHookManagerValue(tc.in.ctx))
		})
	}
}

func TestWithWorkerHookManagerContext(t *testing.T) {
	// Define structures for input and output.
	type in struct {
		ctx     context.Context
		manager WorkerHookManager
	}
	type out struct {
		ctx context.Context
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: a worker hook manager is set in context",
			in: &in{
				ctx:     context.Background(),
				manager: NewWorkerHookManager(nil),
			},
			out: &out{
				ctx: context.WithValue(context.Background(), workerHookManagerKey{}, NewWorkerHookManager(nil)),
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		t.Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out.ctx, WithWorkerHookManagerContext(tc.in.ctx, tc.in.manager))
		})
	}
}
