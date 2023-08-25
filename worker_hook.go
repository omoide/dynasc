package dynasc

import (
	"context"
	"log/slog"
	"sync"

	"github.com/cockroachdb/errors"
)

// WorkerHooks represents lifecycle hooks of a worker.
type WorkerHooks struct {
	PreSetup              func(ctx context.Context) error
	PostSetup             func(ctx context.Context) error
	PreStreamWorkerSetup  func(ctx context.Context, streamARN string) error
	PostStreamWorkerSetup func(ctx context.Context, streamARN string) error
	PreShardWorkerSetup   func(ctx context.Context, streamARN, shardID string) error
	PostShardWorkerSetup  func(ctx context.Context, streamARN, shardID string) error
}

// WorkerHookManager represents a manager of worker lifecycle hooks.
type WorkerHookManager interface {
	executePreSetup(ctx context.Context) error
	executePostSetup(ctx context.Context) error
	executePreStreamWorkerSetup(ctx context.Context, streamARN string) error
	executePostStreamWorkerSetup(ctx context.Context, streamARN string) error
	executePreShardWorkerSetup(ctx context.Context, streamARN, shardID string) error
	executePostShardWorkerSetup(ctx context.Context, streamARN, shardID string) error
}

// DefaultWorkerHookManager represents a default manager of worker lifecycle hooks.
type DefaultWorkerHookManager struct {
	mu                    *sync.RWMutex
	streamWaitGroups      *TypedSyncMap[string, *sync.WaitGroup]
	shardWaitGroups       *TypedSyncMap[string, *TypedSyncMap[string, *sync.WaitGroup]]
	preSetup              func(ctx context.Context) error
	postSetup             func(ctx context.Context) error
	preStreamWorkerSetup  func(ctx context.Context, streamARN string) error
	postStreamWorkerSetup func(ctx context.Context, streamARN string) error
	preShardWorkerSetup   func(ctx context.Context, streamARN, shardID string) error
	postShardWorkerSetup  func(ctx context.Context, streamARN, shardID string) error
}

// NewWorkerHookManager returns a default manager of worker lifecycle hooks.
func NewWorkerHookManager(hooks *WorkerHooks) *DefaultWorkerHookManager {
	if hooks != nil {
		return &DefaultWorkerHookManager{
			mu:                    &sync.RWMutex{},
			streamWaitGroups:      &TypedSyncMap[string, *sync.WaitGroup]{},
			shardWaitGroups:       &TypedSyncMap[string, *TypedSyncMap[string, *sync.WaitGroup]]{},
			preSetup:              hooks.PreSetup,
			postSetup:             hooks.PostSetup,
			preStreamWorkerSetup:  hooks.PreStreamWorkerSetup,
			postStreamWorkerSetup: hooks.PostStreamWorkerSetup,
			preShardWorkerSetup:   hooks.PreShardWorkerSetup,
			postShardWorkerSetup:  hooks.PostShardWorkerSetup,
		}
	}
	return &DefaultWorkerHookManager{
		mu:               &sync.RWMutex{},
		streamWaitGroups: &TypedSyncMap[string, *sync.WaitGroup]{},
		shardWaitGroups:  &TypedSyncMap[string, *TypedSyncMap[string, *sync.WaitGroup]]{},
	}
}

func (m *DefaultWorkerHookManager) executePreSetup(ctx context.Context) error {
	// Execute a hook.
	if m.preSetup != nil {
		if err := m.preSetup(ctx); err != nil {
			return errors.Wrap(err, "failed to execute a pre setup function")
		}
	}
	slog.Info("All worker setups have started.")
	return nil
}

func (m *DefaultWorkerHookManager) executePostSetup(ctx context.Context) error {
	// Wait until all stream worker wait groups are finalized.
	m.wait(m.streamWaitGroups)
	slog.Info("All worker setups have completed.")
	// Execute a hook.
	if m.postSetup != nil {
		if err := m.postSetup(ctx); err != nil {
			return errors.Wrap(err, "failed to execute a post setup function")
		}
	}
	return nil
}

func (m *DefaultWorkerHookManager) executePreStreamWorkerSetup(ctx context.Context, streamARN string) error {
	// Initialize a stream worker wait group.
	m.streamWaitGroups.Store(streamARN, func() *sync.WaitGroup {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		return wg
	}())
	m.shardWaitGroups.Store(streamARN, &TypedSyncMap[string, *sync.WaitGroup]{})
	// Execute a hook.
	if m.preStreamWorkerSetup != nil {
		if err := m.preStreamWorkerSetup(ctx, streamARN); err != nil {
			return errors.Wrap(err, "failed to execute a pre stream worker setup function")
		}
	}
	slog.Info("Stream worker setup has started.", "streamARN", streamARN)
	return nil
}

func (m *DefaultWorkerHookManager) executePostStreamWorkerSetup(ctx context.Context, streamARN string) error {
	// Wait until all shard worker wait groups are finalized.
	if wgs, loaded := m.shardWaitGroups.Load(streamARN); loaded {
		m.wait(wgs)
	}
	slog.Info("Stream worker setup has completed.", "streamARN", streamARN)
	// Execute a hook.
	if m.postStreamWorkerSetup != nil {
		if err := m.postStreamWorkerSetup(ctx, streamARN); err != nil {
			return errors.Wrap(err, "failed to execute a post stream worker setup function")
		}
	}
	// Finalize a stream worker wait group.
	if wg, loaded := m.streamWaitGroups.Load(streamARN); loaded {
		wg.Done()
	}
	return nil
}

func (m *DefaultWorkerHookManager) executePreShardWorkerSetup(ctx context.Context, streamARN, shardID string) error {
	// Initialize a shard worker wait group.
	if wgs, loaded := m.shardWaitGroups.Load(streamARN); loaded {
		wgs.Store(shardID, func() *sync.WaitGroup {
			wg := &sync.WaitGroup{}
			wg.Add(1)
			return wg
		}())
	}
	// Execute a hook.
	if m.preShardWorkerSetup != nil {
		if err := m.preShardWorkerSetup(ctx, streamARN, shardID); err != nil {
			return errors.Wrap(err, "failed to execute a pre shard worker setup function")
		}
	}
	slog.Info("Shard worker setup has started.", "streamARN", streamARN, "shardID", shardID)
	return nil
}

func (m *DefaultWorkerHookManager) executePostShardWorkerSetup(ctx context.Context, streamARN, shardID string) error {
	slog.Info("Shard worker setup has completed.", "streamARN", streamARN, "shardID", shardID)
	// Execute a hook.
	if m.postShardWorkerSetup != nil {
		if err := m.postShardWorkerSetup(ctx, streamARN, shardID); err != nil {
			return errors.Wrap(err, "failed to execute a post shard worker setup function")
		}
	}
	// Finalize a shard worker wait group.
	if wgs, loaded := m.shardWaitGroups.Load(streamARN); loaded {
		if wg, loaded := wgs.Load(shardID); loaded {
			wg.Done()
		}
	}
	return nil
}

func (m *DefaultWorkerHookManager) wait(wgs *TypedSyncMap[string, *sync.WaitGroup]) {
	wgs.Range(func(key string, wg *sync.WaitGroup) bool {
		wg.Wait()
		return true
	})
}

// NoopWorkerHookManager represents a manager of worker lifecycle hooks that executes noting.
type NoopWorkerHookManager struct {
}

// NewNoopWorkerHookManager returns a manager of worker lifecycle hooks that executes noting.
func NewNoopWorkerHookManager() *NoopWorkerHookManager {
	return &NoopWorkerHookManager{}
}

func (m *NoopWorkerHookManager) executePreSetup(ctx context.Context) error {
	return nil
}

func (m *NoopWorkerHookManager) executePostSetup(ctx context.Context) error {
	return nil
}

func (m *NoopWorkerHookManager) executePreStreamWorkerSetup(ctx context.Context, streamARN string) error {
	return nil
}

func (m *NoopWorkerHookManager) executePostStreamWorkerSetup(ctx context.Context, streamARN string) error {
	return nil
}

func (m *NoopWorkerHookManager) executePreShardWorkerSetup(ctx context.Context, streamARN, shardID string) error {
	return nil
}

func (m *NoopWorkerHookManager) executePostShardWorkerSetup(ctx context.Context, streamARN, shardID string) error {
	return nil
}

type workerHookManagerKey struct{}

// WorkerHookManagerValue returns the worker hook manager associated with the context.
func WorkerHookManagerValue(ctx context.Context) WorkerHookManager {
	if manager, ok := ctx.Value(workerHookManagerKey{}).(WorkerHookManager); ok {
		return manager
	}
	return NewNoopWorkerHookManager()
}

// WithWorkerHookManagerContext returns a context with the specified worker hook manager associated.
func WithWorkerHookManagerContext(ctx context.Context, manager WorkerHookManager) context.Context {
	return context.WithValue(ctx, workerHookManagerKey{}, manager)
}
