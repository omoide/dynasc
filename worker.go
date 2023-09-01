package dynasc

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Worker represents a DynamoDB Streams worker.
type Worker struct {
	db        DBClient
	dbstreams DBStreamsClient
	processor Processor
	batchSize *int32
	hooks     *WorkerHooks
}

// WorkerHooks represents lifecycle hooks of a worker.
type WorkerHooks struct {
	PreSetup  func(ctx context.Context) error
	PostSetup func(ctx context.Context) error
}

// NewWorker returns an instance of a worker.
func NewWorker(config *WorkerConfig, options ...WorkerConfigOption) *Worker {
	w := &Worker{
		db:        config.DB,
		dbstreams: config.DBStreams,
		processor: config.Processor,
		batchSize: Int32Ptr(100),
		hooks:     &WorkerHooks{},
	}
	for _, option := range options {
		option(w)
	}
	return w
}

// Execute executes a worker.
func (w *Worker) Execute(ctx context.Context, triggers map[string][]string) error {
	// Execute post setup function if specified.
	if w.hooks.PreSetup != nil {
		if err := w.hooks.PreSetup(ctx); err != nil {
			return errors.Wrap(err, "failed to execute a pre setup function")
		}
	}
	// Set up the triggers.
	// Generate a new trigger definition consisting of a map of shard iterators and function names.
	triggers, err := w.setup(ctx, triggers)
	if err != nil {
		return err
	}
	// Execute post setup function if specified.
	if w.hooks.PostSetup != nil {
		if err := w.hooks.PostSetup(ctx); err != nil {
			return errors.Wrap(err, "failed to execute a post setup function")
		}
	}
	// Execute the workers.
	return w.executeWorkers(ctx, triggers)
}

// setup setups shard iterators and return them with their corresponding function names.
func (w *Worker) setup(ctx context.Context, triggers map[string][]string) (map[string][]string, error) {
	mu := sync.RWMutex{}
	eg, ctx := errgroup.WithContext(ctx)
	result := map[string][]string{}
	for k, v := range triggers {
		tableName, functionNames := k, v
		eg.Go(func() error {
			// Get the stream ARN
			streamARN, err := w.streamARN(ctx, tableName)
			if err != nil {
				return errors.Wrapf(err, "failed to get the stream ARN for the specified table [tableName: %s]", tableName)
			}
			// Get the shards.
			shards, err := w.shards(ctx, StringValue(streamARN), nil)
			if err != nil {
				return errors.Wrapf(err, "failed to get the shards of the specified stream [tableName: %s] [streamARN: %s]", tableName, streamARN)
			}
			// Get the shard iterators.
			for _, shard := range shards {
				itor, err := w.shardIterator(ctx, StringValue(streamARN), StringValue(shard.ShardId))
				if err != nil {
					return errors.Wrapf(err, "failed to get the shard iterators for the specified shard [tableName: %s] [streamARN: %s] [shardID: %s]", tableName, streamARN, StringValue(streamARN), StringValue(shard.ShardId))
				}
				// Set the set of function names for each iterator.
				mu.Lock()
				result[StringValue(itor)] = functionNames
				mu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Wrap(err, "failed to setup some triggers")
	}
	return result, nil
}

// execute executes workers.
func (w *Worker) executeWorkers(ctx context.Context, triggers map[string][]string) error {
	// Execute shard workers.
	eg, ctx := errgroup.WithContext(ctx)
	for k, v := range triggers {
		itor, functionNames := k, v
		eg.Go(func() error {
			return w.executeShardWorker(ctx, itor, functionNames)
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "failed to execute some shard workers")
	}
	return nil
}

// executeShardWorker executes a shard worker.
func (w *Worker) executeShardWorker(ctx context.Context, itor string, functionNames []string) error {
	limiter := rate.NewLimiter(rate.Every(time.Second/4), 1)
	for next := &itor; next != nil; {
		if err := limiter.Wait(ctx); err != nil {
			return errors.Wrap(err, "failed to wait")
		}
		// Get records.
		out, err := w.dbstreams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: next,
			Limit:         w.batchSize,
		})
		if err != nil {
			return errors.Wrap(err, "failed to execute DynamoDBStreams#GetRecords")
		}
		// Continue if the record count is 0.
		if len(out.Records) == 0 {
			next = out.NextShardIterator
			continue
		}
		// Process records.
		eg, ctx := errgroup.WithContext(ctx)
		for _, v := range functionNames {
			name := v
			eg.Go(func() error {
				return w.processor.Process(ctx, name, out.Records)
			})
		}
		if err := eg.Wait(); err != nil {
			return errors.Wrap(err, "failed to process some records")
		}
		next = out.NextShardIterator
	}
	return nil
}

// streamARN returns the latest stream ARN for the specified table.
func (w *Worker) streamARN(ctx context.Context, tableName string) (*string, error) {
	out, err := w.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: StringPtr(tableName),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute DynamoDB#DescribeTable")
	}
	return out.Table.LatestStreamArn, nil
}

// shards returns the shards for the specified stream.
func (w *Worker) shards(ctx context.Context, streamARN string, lastEvaluatedShardID *string) ([]dbstreamstypes.Shard, error) {
	out, err := w.dbstreams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn:             &streamARN,
		ExclusiveStartShardId: lastEvaluatedShardID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute DynamoDBStreams#DescribeStream")
	}
	shards := []dbstreamstypes.Shard{}
	for _, shard := range out.StreamDescription.Shards {
		// Append only the opened shard.
		if shard.SequenceNumberRange.EndingSequenceNumber != nil {
			continue
		}
		shards = append(shards, shard)
	}
	// Get shard IDs that have not yet been retrieved, if the last evaluated shard ID is not empty.
	if shardID := out.StreamDescription.LastEvaluatedShardId; shardID != nil {
		extraShards, err := w.shards(ctx, streamARN, shardID)
		if err != nil {
			return nil, err
		}
		return append(shards, extraShards...), nil
	}
	return shards, nil
}

// shardIterator returns the iterator for the specified shard.
func (w *Worker) shardIterator(ctx context.Context, streamARN, shardID string) (*string, error) {
	out, err := w.dbstreams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         &streamARN,
		ShardId:           &shardID,
		ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute DynamoDBStreams#GetShardIterators")
	}
	return out.ShardIterator, nil
}
