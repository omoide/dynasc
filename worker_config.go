package dynasc

// WorkerConfig represents a configuration to execute a worker.
type WorkerConfig struct {
	DB        DBClient
	DBStreams DBStreamsClient
	Processor Processor
}

// WorkerConfigOption represents a function to set optional configuration.
type WorkerConfigOption func(*Worker)

// WithBatchSize returns a function that set batch size to a worker.
func WithBatchSize(batchSize int32) WorkerConfigOption {
	return func(w *Worker) {
		w.batchSize = &batchSize
	}
}

// WithBatchSize returns a function that set hooks to a worker.
func WithHooks(hooks *WorkerHooks) WorkerConfigOption {
	return func(w *Worker) {
		if hooks != nil {
			w.hooks = hooks
		}
	}
}
