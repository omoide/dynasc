package dynasc

import (
	"context"
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
	triggers  map[string][]string
	batchSize *int32
	hooks     *WorkerHooks
}

// NewWorker returns an instance of a worker.
func NewWorker(config *WorkerConfig, options ...WorkerConfigOption) *Worker {
	w := &Worker{
		db:        config.DB,
		dbstreams: config.DBStreams,
		processor: config.Processor,
		triggers:  config.Triggers,
		batchSize: Int32Ptr(100),
		hooks:     &WorkerHooks{},
	}
	for _, option := range options {
		option(w)
	}
	return w
}

// Execute executes a worker.
func (w *Worker) Execute(ctx context.Context) error {
	return w.execute(WithWorkerHookManagerContext(ctx, NewWorkerHookManager(w.hooks)))
}

// execute executes a worker.
func (w *Worker) execute(ctx context.Context) error {
	WorkerHookManagerValue(ctx).executePreSetup(ctx)
	// Generate triggers with stream ARN as the key.
	triggers, err := ParallelMapToMapE(ctx, w.triggers, func(ctx context.Context, tableName string, functionNames []string) (string, []string, error) {
		streamARN, err := w.streamARN(ctx, tableName)
		if err != nil {
			return "", nil, errors.Wrapf(err, "failed to get the stream ARN for %s table", tableName)
		}
		return StringValue(streamARN), functionNames, nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to generate triggers with stream ARN as the key")
	}
	// Execute workers for the number of table streams.
	eg, ctx := errgroup.WithContext(ctx)
	for streamARN, functionNames := range triggers {
		WorkerHookManagerValue(ctx).executePreStreamWorkerSetup(ctx, streamARN)
		eg.Go(func(streamARN string, functionNames []string) func() error {
			return func() error {
				return w.executeStreamWorker(ctx, streamARN, functionNames)
			}
		}(streamARN, functionNames))
	}
	eg.Go(func() error {
		WorkerHookManagerValue(ctx).executePostSetup(ctx)
		return nil
	})
	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "failed to process streams")
	}
	return nil
}

// executeStreamWorker executes a stream worker.
func (w *Worker) executeStreamWorker(ctx context.Context, streamARN string, functionNames []string) error {
	shards, err := w.shards(ctx, streamARN, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to get the shards of the specified stream: %s", streamARN)
	}
	// Execute workers for the number of shards.
	eg, ctx := errgroup.WithContext(ctx)
	for _, shard := range shards {
		WorkerHookManagerValue(ctx).executePreShardWorkerSetup(ctx, streamARN, StringValue(shard.ShardId))
		eg.Go(func(shardID *string) func() error {
			return func() error {
				return w.executeShardWorker(ctx, streamARN, StringValue(shardID), functionNames)
			}
		}(shard.ShardId))
	}
	eg.Go(func() error {
		WorkerHookManagerValue(ctx).executePostStreamWorkerSetup(ctx, streamARN)
		return nil
	})
	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "failed to process shards")
	}
	return nil
}

// executeShardWorker executes a shard worker.
func (w *Worker) executeShardWorker(ctx context.Context, streamARN, shardID string, functionNames []string) error {
	limiter := rate.NewLimiter(rate.Every(time.Second/4), 1)
	// Get a shard iterator.
	itor, err := w.dbstreams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         &streamARN,
		ShardId:           &shardID,
		ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
	})
	if err != nil {
		return errors.Wrap(err, "failed to execute DynamoDBStreams#GetShardIterators")
	}
	WorkerHookManagerValue(ctx).executePostShardWorkerSetup(ctx, streamARN, shardID)
	for next := itor.ShardIterator; next != nil; {
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
		for _, name := range functionNames {
			eg.Go(func(name string) func() error {
				return func() error {
					return w.processor.Process(ctx, name, out.Records)
				}
			}(name))
		}
		if err := eg.Wait(); err != nil {
			return errors.Wrap(err, "failed to process records")
		}
		next = out.NextShardIterator
	}
	return nil
}

// streamARN returns the Amazon Resource Name (ARN) that uniquely identifies the latest stream for specified table.
func (w *Worker) streamARN(ctx context.Context, tableName string) (*string, error) {
	out, err := w.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: StringPtr(tableName),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute DynamoDB#DescribeTable")
	}
	return out.Table.LatestStreamArn, nil
}

// shards returns the uniquely identified group of stream records within a stream.
func (w *Worker) shards(ctx context.Context, streamARN string, lastEvaluatedShardID *string) ([]dbstreamstypes.Shard, error) {
	out, err := w.dbstreams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn:             &streamARN,
		ExclusiveStartShardId: lastEvaluatedShardID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute DynamoDBStreams#DescribeStream")
	}
	// Get shard IDs that have not yet been retrieved, if the last evaluated shard ID is not empty.
	if shardID := out.StreamDescription.LastEvaluatedShardId; shardID != nil {
		shards, err := w.shards(ctx, streamARN, shardID)
		if err != nil {
			return nil, err
		}
		return append(out.StreamDescription.Shards, shards...), nil
	}
	return out.StreamDescription.Shards, nil
}
