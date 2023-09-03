package dynasc

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type WorkerTestSuite struct {
	suite.Suite
}

func (s *WorkerTestSuite) TestExecute() {
	// Define structures for input and output.
	type in struct {
		triggers map[string][]string
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name         string
		in           *in
		out          *out
		hooks        *WorkerHooks
		mockDBClient func() DBClient
	}{
		{
			name: "Normal: a pre setup function is specified",
			in: &in{
				triggers: map[string][]string{},
			},
			out: &out{},
			hooks: &WorkerHooks{
				PreSetup: func(ctx context.Context) error {
					return nil
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				return m
			},
		},
		{
			name: "Normal: a post setup function is specified",
			in: &in{
				triggers: map[string][]string{},
			},
			out: &out{},
			hooks: &WorkerHooks{
				PostSetup: func(ctx context.Context) error {
					return nil
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				return m
			},
		},
		{
			name: "Abnormal: a pre setup function returns an error",
			in: &in{
				triggers: map[string][]string{},
			},
			out: &out{
				err: errors.New("failed to execute a pre setup function"),
			},
			hooks: &WorkerHooks{
				PreSetup: func(ctx context.Context) error {
					return errors.New("")
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				return m
			},
		},
		{
			name: "Abnormal: Worker#setup returns an error",
			in: &in{
				triggers: map[string][]string{
					"TableName": {"FunctionName"}},
			},
			out: &out{
				err: errors.New("failed to setup some triggers"),
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
		{
			name: "Abnormal: a post setup function returns an error",
			in: &in{
				triggers: map[string][]string{},
			},
			out: &out{
				err: errors.New("failed to execute a post setup function"),
			},
			hooks: &WorkerHooks{
				PostSetup: func(ctx context.Context) error {
					return errors.New("")
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DB: tc.mockDBClient(),
			}, WithHooks(tc.hooks))
			err := w.Execute(ctx, tc.in.triggers)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *WorkerTestSuite) TestSetup() {
	// Define structures for input and output.
	type in struct {
		triggers map[string][]string
	}
	type out struct {
		triggers map[string][]string
		err      error
	}
	// Define test cases.
	tests := []struct {
		name                string
		in                  *in
		out                 *out
		mockDBClient        func() DBClient
		mockDBStreamsClient func() DBStreamsClient
	}{
		{
			name: "Normal: triggers contain a single table and a single shard iterator exists for the table",
			in: &in{
				triggers: map[string][]string{
					"TableName": {"FunctionName1", "FunctionName2"},
				},
			},
			out: &out{
				triggers: map[string][]string{
					"shardId-00000000000000000000-00000000-itor": {"FunctionName1", "FunctionName2"},
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000000"),
					},
				}, nil)
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				m.On("GetShardIterator", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
				}, nil)
				return m
			},
		},
		{
			name: "Normal: triggers contain a single table and multiple shard iterators exist for the table",
			in: &in{
				triggers: map[string][]string{
					"TableName": {"FunctionName1", "FunctionName2"},
				},
			},
			out: &out{
				triggers: map[string][]string{
					"shardId-00000000000000000000-00000000-itor": {"FunctionName1", "FunctionName2"},
					"shardId-00000000000000000000-00000001-itor": {"FunctionName1", "FunctionName2"},
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000000"),
					},
				}, nil)
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000001"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				m.On("GetShardIterator", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
				}, nil)
				m.On("GetShardIterator", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000001"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000001-itor"),
				}, nil)
				return m
			},
		},
		{
			name: "Normal: triggers contain multiple tables",
			in: &in{
				triggers: map[string][]string{
					"TableName1": {"FunctionName1", "FunctionName2"},
					"TableName2": {"FunctionName1"},
				},
			},
			out: &out{
				triggers: map[string][]string{
					"shardId-00000000000000000000-00000000-itor": {"FunctionName1", "FunctionName2"},
					"shardId-00000000000000000001-00000000-itor": {"FunctionName1"},
				},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName1"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000000"),
					},
				}, nil)
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName2"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000001"),
					},
				}, nil)
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				m.On("DescribeStream", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000001"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000001-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				m.On("GetShardIterator", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
				}, nil)
				m.On("GetShardIterator", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000001"),
					ShardId:           StringPtr("shardId-00000000000000000001-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardId-00000000000000000001-00000000-itor"),
				}, nil)
				return m
			},
		},
		{
			name: "Normal: triggers contain no table",
			in: &in{
				triggers: map[string][]string{},
			},
			out: &out{
				triggers: map[string][]string{},
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				return m
			},
		},
		{
			name: "Abnormal: Worker#streamARN returns an error",
			in: &in{
				triggers: map[string][]string{
					"TableName": {"FunctionName1", "FunctionName2"},
				},
			},
			out: &out{
				triggers: nil,
				err:      errors.New("failed to get the stream ARN for the specified table"),
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				return m
			},
		},
		{
			name: "Abnormal: Worker#shards returns an error",
			in: &in{
				triggers: map[string][]string{
					"TableName": {"FunctionName1", "FunctionName2"},
				},
			},
			out: &out{
				triggers: nil,
				err:      errors.New("failed to get the shards of the specified stream"),
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000000"),
					},
				}, nil)
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
		{
			name: "Abnormal: Worker#shardIterator returns an error",
			in: &in{
				triggers: map[string][]string{
					"TableName": {"FunctionName1", "FunctionName2"},
				},
			},
			out: &out{
				triggers: nil,
				err:      errors.New("failed to get the shard iterators for the specified shard"),
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", mock.AnythingOfType("*context.cancelCtx"), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000000"),
					},
				}, nil)
				return m
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				m.On("GetShardIterator", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DB:        tc.mockDBClient(),
				DBStreams: tc.mockDBStreamsClient(),
			})
			triggers, err := w.setup(ctx, tc.in.triggers)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.out.triggers, triggers)
			}
		})
	}
}

func (s *WorkerTestSuite) TestExecuteWorkers() {
	// Define structures for input and output.
	type in struct {
		triggers map[string][]string
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name                string
		in                  *in
		out                 *out
		mockDBStreamsClient func() DBStreamsClient
		mockProcessor       func() Processor
	}{
		{
			name: "Normal: triggers contains a single shard iterator",
			in: &in{
				triggers: map[string][]string{
					"shardId-00000000000000000000-00000000-itor": {"FunctionName"},
				},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []dbstreamstypes.Record{
						{
							AwsRegion:    StringPtr("ap-northeast-1"),
							EventID:      StringPtr("EventID"),
							EventName:    dbstreamstypes.OperationTypeInsert,
							EventSource:  StringPtr("aws:dynamodb"),
							EventVersion: StringPtr("1"),
							Dynamodb: &dbstreamstypes.StreamRecord{
								ApproximateCreationDateTime: &time.Time{},
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								SequenceNumber: StringPtr("000000000000000000000001"),
								SizeBytes:      Int64Ptr(256),
								StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
							},
							UserIdentity: &dbstreamstypes.Identity{
								Type:        StringPtr("Service"),
								PrincipalId: StringPtr("dynamodb.amazonaws.com"),
							},
						},
					},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000001"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(nil)
				return m
			},
		},
		{
			name: "Normal: triggers contain multiple shard iterators",
			in: &in{
				triggers: map[string][]string{
					"shardId-00000000000000000000-00000000-itor": {"FunctionName"},
					"shardId-00000000000000000000-00000001-itor": {"FunctionName"},
				},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []dbstreamstypes.Record{
						{
							AwsRegion:    StringPtr("ap-northeast-1"),
							EventID:      StringPtr("EventID"),
							EventName:    dbstreamstypes.OperationTypeInsert,
							EventSource:  StringPtr("aws:dynamodb"),
							EventVersion: StringPtr("1"),
							Dynamodb: &dbstreamstypes.StreamRecord{
								ApproximateCreationDateTime: &time.Time{},
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								SequenceNumber: StringPtr("000000000000000000000001"),
								SizeBytes:      Int64Ptr(256),
								StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
							},
							UserIdentity: &dbstreamstypes.Identity{
								Type:        StringPtr("Service"),
								PrincipalId: StringPtr("dynamodb.amazonaws.com"),
							},
						},
					},
				}, nil)
				m.On("GetRecords", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000001-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []dbstreamstypes.Record{
						{
							AwsRegion:    StringPtr("ap-northeast-1"),
							EventID:      StringPtr("EventID"),
							EventName:    dbstreamstypes.OperationTypeInsert,
							EventSource:  StringPtr("aws:dynamodb"),
							EventVersion: StringPtr("1"),
							Dynamodb: &dbstreamstypes.StreamRecord{
								ApproximateCreationDateTime: &time.Time{},
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00002",
									},
								},
								SequenceNumber: StringPtr("000000000000000000000002"),
								SizeBytes:      Int64Ptr(256),
								StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
							},
							UserIdentity: &dbstreamstypes.Identity{
								Type:        StringPtr("Service"),
								PrincipalId: StringPtr("dynamodb.amazonaws.com"),
							},
						},
					},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000001"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(nil)
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00002",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000002"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(nil)
				return m
			},
		},
		{
			name: "Normal: triggers contain no shard iterator",
			in: &in{
				triggers: map[string][]string{},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: Worker#executeShardWorker returns an error",
			in: &in{
				triggers: map[string][]string{
					"shardId-00000000000000000000-00000000-itor": {"FunctionName"},
				},
			},
			out: &out{
				err: errors.New("failed to execute some shard workers"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DB:        nil,
				DBStreams: tc.mockDBStreamsClient(),
				Processor: tc.mockProcessor(),
			})
			err := w.executeWorkers(ctx, tc.in.triggers)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *WorkerTestSuite) TestExecuteShardWorker() {
	// Define structures for input and output.
	type in struct {
		ctx           context.Context
		itor          string
		functionNames []string
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name                string
		in                  *in
		out                 *out
		mockDBStreamsClient func() DBStreamsClient
		mockProcessor       func() Processor
	}{
		{
			name: "Normal: a single record exists",
			in: &in{
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []dbstreamstypes.Record{
						{
							AwsRegion:    StringPtr("ap-northeast-1"),
							EventID:      StringPtr("EventID"),
							EventName:    dbstreamstypes.OperationTypeInsert,
							EventSource:  StringPtr("aws:dynamodb"),
							EventVersion: StringPtr("1"),
							Dynamodb: &dbstreamstypes.StreamRecord{
								ApproximateCreationDateTime: &time.Time{},
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								NewImage: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								OldImage: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								SequenceNumber: StringPtr("000000000000000000000001"),
								SizeBytes:      Int64Ptr(256),
								StreamViewType: dbstreamstypes.StreamViewTypeNewAndOldImages,
							},
							UserIdentity: &dbstreamstypes.Identity{
								Type:        StringPtr("Service"),
								PrincipalId: StringPtr("dynamodb.amazonaws.com"),
							},
						},
					},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							NewImage: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							OldImage: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000001"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeNewAndOldImages,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(nil)
				return m
			},
		},
		{
			name: "Normal: no record exists",
			in: &in{
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records:           []dbstreamstypes.Record{},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Normal: multiple functions are specified",
			in: &in{
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName1", "FunctionName2"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []dbstreamstypes.Record{
						{
							AwsRegion:    StringPtr("ap-northeast-1"),
							EventID:      StringPtr("EventID"),
							EventName:    dbstreamstypes.OperationTypeInsert,
							EventSource:  StringPtr("aws:dynamodb"),
							EventVersion: StringPtr("1"),
							Dynamodb: &dbstreamstypes.StreamRecord{
								ApproximateCreationDateTime: &time.Time{},
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								SequenceNumber: StringPtr("000000000000000000000001"),
								SizeBytes:      Int64Ptr(256),
								StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
							},
							UserIdentity: &dbstreamstypes.Identity{
								Type:        StringPtr("Service"),
								PrincipalId: StringPtr("dynamodb.amazonaws.com"),
							},
						},
					},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName1", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000001"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(nil)
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName2", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000001"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(nil)
				return m
			},
		},
		{
			name: "Normal: a next shard iterator is specified",
			in: &in{
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: StringPtr("shardId-00000000000000000000-00000001-itor"),
					Records:           []dbstreamstypes.Record{},
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000001-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records:           []dbstreamstypes.Record{},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: Limiter#Wait returns an error",
			in: &in{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				}(),
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to wait"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: DynamoDBStreams#GetRecords returns an error",
			in: &in{
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to execute DynamoDBStreams#GetRecords"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: Processor#Process returns an error",
			in: &in{
				itor:          "shardId-00000000000000000000-00000000-itor",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to process some records"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []dbstreamstypes.Record{
						{
							AwsRegion:    StringPtr("ap-northeast-1"),
							EventID:      StringPtr("EventID"),
							EventName:    dbstreamstypes.OperationTypeInsert,
							EventSource:  StringPtr("aws:dynamodb"),
							EventVersion: StringPtr("1"),
							Dynamodb: &dbstreamstypes.StreamRecord{
								ApproximateCreationDateTime: &time.Time{},
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{
										Value: "00001",
									},
								},
								SequenceNumber: StringPtr("000000000000000000000001"),
								SizeBytes:      Int64Ptr(256),
								StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
							},
							UserIdentity: &dbstreamstypes.Identity{
								Type:        StringPtr("Service"),
								PrincipalId: StringPtr("dynamodb.amazonaws.com"),
							},
						},
					},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", []dbstreamstypes.Record{
					{
						AwsRegion:    StringPtr("ap-northeast-1"),
						EventID:      StringPtr("EventID"),
						EventName:    dbstreamstypes.OperationTypeInsert,
						EventSource:  StringPtr("aws:dynamodb"),
						EventVersion: StringPtr("1"),
						Dynamodb: &dbstreamstypes.StreamRecord{
							ApproximateCreationDateTime: &time.Time{},
							Keys: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
							},
							SequenceNumber: StringPtr("000000000000000000000001"),
							SizeBytes:      Int64Ptr(256),
							StreamViewType: dbstreamstypes.StreamViewTypeKeysOnly,
						},
						UserIdentity: &dbstreamstypes.Identity{
							Type:        StringPtr("Service"),
							PrincipalId: StringPtr("dynamodb.amazonaws.com"),
						},
					},
				}).Return(errors.New(""))
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := func() context.Context {
			if tc.in.ctx != nil {
				return tc.in.ctx
			}
			return context.Background()
		}()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DB:        nil,
				DBStreams: tc.mockDBStreamsClient(),
				Processor: tc.mockProcessor(),
			})
			err := w.executeShardWorker(ctx, tc.in.itor, tc.in.functionNames)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *WorkerTestSuite) TestStreamARN() {
	// Define structures for input and output.
	type in struct {
		tableName string
	}
	type out struct {
		streamARN *string
		err       error
	}
	// Define test cases.
	tests := []struct {
		name         string
		in           *in
		out          *out
		mockDBClient func() DBClient
	}{
		{
			name: "Normal: a specified table exists",
			in: &in{
				tableName: "TableName",
			},
			out: &out{
				streamARN: StringPtr("arn:aws:dynamodb:000000000000"),
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", context.Background(), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(&dynamodb.DescribeTableOutput{
					Table: &dbtypes.TableDescription{
						LatestStreamArn: StringPtr("arn:aws:dynamodb:000000000000"),
					},
				}, nil)
				return m
			},
		},
		{
			name: "Abnormal: DynamoDB#DescribeTable returns an error",
			in: &in{
				tableName: "TableName",
			},
			out: &out{
				streamARN: nil,
				err:       errors.New("failed to execute DynamoDB#DescribeTable"),
			},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				m.On("DescribeTable", context.Background(), &dynamodb.DescribeTableInput{
					TableName: StringPtr("TableName"),
				}, ([]func(*dynamodb.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DB: tc.mockDBClient(),
			})
			streamARN, err := w.streamARN(ctx, tc.in.tableName)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.out.streamARN, streamARN)
			}
		})
	}
}

func (s *WorkerTestSuite) TestShards() {
	// Define structures for input and output.
	type in struct {
		streamARN string
	}
	type out struct {
		shards []dbstreamstypes.Shard
		err    error
	}
	// Define test cases.
	tests := []struct {
		name                string
		in                  *in
		out                 *out
		mockDBStreamsClient func() DBStreamsClient
	}{
		{
			name: "Normal: a shard is opened",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
			},
			out: &out{
				shards: []dbstreamstypes.Shard{
					{
						ShardId: StringPtr("shardId-00000000000000000000-00000000"),
						SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
							StartingSequenceNumber: StringPtr("0"),
						},
					},
				},
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				return m
			},
		},
		{
			name: "Normal: a shard is closed",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
			},
			out: &out{
				shards: []dbstreamstypes.Shard{},
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
									EndingSequenceNumber:   StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				return m
			},
		},
		{
			name: "Normal: a last evaluated shard ID exists",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
			},
			out: &out{
				shards: []dbstreamstypes.Shard{
					{
						ShardId: StringPtr("shardId-00000000000000000000-00000000"),
						SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
							StartingSequenceNumber: StringPtr("0"),
						},
					},
					{
						ShardId: StringPtr("shardId-00000000000000000000-00000001"),
						SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
							StartingSequenceNumber: StringPtr("0"),
						},
					},
				},
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: StringPtr("shardId-00000000000000000000-00000000"),
					},
				}, nil)
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: StringPtr("shardId-00000000000000000000-00000000"),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000001"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				return m
			},
		},
		{
			name: "Abnormal: DynamoDBStreams#DescribeStream returns an error",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
			},
			out: &out{
				shards: nil,
				err:    errors.New("failed to execute DynamoDBStreams#DescribeStream"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
		{
			name: "Abnormal: DynamoDBStreams#DescribeStream returns an error at the second call",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
			},
			out: &out{
				shards: nil,
				err:    errors.New("failed to execute DynamoDBStreams#DescribeStream"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards: []dbstreamstypes.Shard{
							{
								ShardId: StringPtr("shardId-00000000000000000000-00000000"),
								SequenceNumberRange: &dbstreamstypes.SequenceNumberRange{
									StartingSequenceNumber: StringPtr("0"),
								},
							},
						},
						LastEvaluatedShardId: StringPtr("shardId-00000000000000000000-00000000"),
					},
				}, nil)
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: StringPtr("shardId-00000000000000000000-00000000"),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DBStreams: tc.mockDBStreamsClient(),
			})
			shards, err := w.shards(ctx, tc.in.streamARN, nil)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.out.shards, shards)
			}
		})
	}
}

func (s *WorkerTestSuite) TestShardIterator() {
	// Define structures for input and output.
	type in struct {
		streamARN string
		shardID   string
	}
	type out struct {
		itor *string
		err  error
	}
	// Define test cases.
	tests := []struct {
		name                string
		in                  *in
		out                 *out
		mockDBStreamsClient func() DBStreamsClient
	}{
		{
			name: "Normal: a shard iterator exists",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
				shardID:   "shardId-00000000000000000000-00000000",
			},
			out: &out{
				itor: StringPtr("shardId-00000000000000000000-00000000-itor"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardId-00000000000000000000-00000000-itor"),
				}, nil)
				return m
			},
		},
		{
			name: "Abnormal: DynamoDBStreams#GetShardIterators returns an error",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
				shardID:   "shardId-00000000000000000000-00000000",
			},
			out: &out{
				itor: nil,
				err:  errors.New("failed to execute DynamoDBStreams#GetShardIterators"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DBStreams: tc.mockDBStreamsClient(),
			})
			itor, err := w.shardIterator(ctx, tc.in.streamARN, tc.in.shardID)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.out.itor, itor)
			}
		})
	}
}

func TestWorkerTestSuite(t *testing.T) {
	// Execute.
	suite.Run(t, &WorkerTestSuite{})
}

type WorkerIntegrationTestSuite struct {
	suite.Suite
	db        *dynamodb.Client
	dbstreams *dynamodbstreams.Client
}

func (s *WorkerIntegrationTestSuite) TestExecute() {
	// Define structures for input and output.
	type in struct {
		triggers func(id string) map[string][]string
	}
	type out struct {
		err error
	}
	// Define the validate function for a record.
	validate := func(expected, actual dbstreamstypes.Record) bool {
		// Validate the event name.
		if !reflect.DeepEqual(expected.EventName, actual.EventName) {
			return false
		}
		// Validate the stream view type.
		if !reflect.DeepEqual(expected.Dynamodb.StreamViewType, actual.Dynamodb.StreamViewType) {
			return false
		}
		// Validate the keys.
		if !reflect.DeepEqual(expected.Dynamodb.Keys, actual.Dynamodb.Keys) {
			return false
		}
		// Validate the new image.
		if !reflect.DeepEqual(expected.Dynamodb.NewImage, actual.Dynamodb.NewImage) {
			return false
		}
		// Validate the old image.
		if !reflect.DeepEqual(expected.Dynamodb.OldImage, actual.Dynamodb.OldImage) {
			return false
		}
		return true
	}
	// Define test cases.
	tests := []struct {
		name          string
		in            *in
		out           *out
		hooks         func(id string) *WorkerHooks
		mockProcessor func() Processor
	}{
		{
			name: "Normal: process a single inserted record",
			in: &in{
				triggers: func(id string) map[string][]string {
					return map[string][]string{
						s.tableName(id): {"FunctionName"},
					}
				},
			},
			out: &out{
				err: errors.New("failed to process some records"),
			},
			hooks: func(id string) *WorkerHooks {
				return &WorkerHooks{
					PostSetup: func(ctx context.Context) error {
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID": &dbtypes.AttributeValueMemberS{Value: "00001"},
							},
						}); err != nil {
							return err
						}
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID": &dbtypes.AttributeValueMemberS{Value: "00002"},
							},
						}); err != nil {
							return err
						}
						return nil
					},
				}
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				// Process a first record.
				c := m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.MatchedBy(func(records []dbstreamstypes.Record) bool {
					// Validate the size of the record.
					if len(records) != 1 {
						return false
					}
					// Validate the record data.
					return validate(
						dbstreamstypes.Record{
							EventName: dbstreamstypes.OperationTypeInsert,
							Dynamodb: &dbstreamstypes.StreamRecord{
								StreamViewType: dbstreamstypes.StreamViewTypeNewAndOldImages,
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
								},
								NewImage: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
								},
								OldImage: (map[string]dbstreamstypes.AttributeValue)(nil),
							},
						},
						records[0],
					)
				})).Return(nil).Once()
				// Process a second record to break out of the loop by returning an error.
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.AnythingOfType("[]types.Record")).Return(errors.New("")).NotBefore(c).Once()
				return m
			},
		},
		{
			name: "Normal: process a single modified record",
			in: &in{
				triggers: func(id string) map[string][]string {
					return map[string][]string{
						s.tableName(id): {"FunctionName"},
					}
				},
			},
			out: &out{
				err: errors.New("failed to process some records"),
			},
			hooks: func(id string) *WorkerHooks {
				return &WorkerHooks{
					PreSetup: func(ctx context.Context) error {
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID":  &dbtypes.AttributeValueMemberS{Value: "00001"},
								"Age": &dbtypes.AttributeValueMemberN{Value: "0"},
							},
						}); err != nil {
							return err
						}
						return nil
					},
					PostSetup: func(ctx context.Context) error {
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID":  &dbtypes.AttributeValueMemberS{Value: "00001"},
								"Age": &dbtypes.AttributeValueMemberN{Value: "1"},
							},
						}); err != nil {
							return err
						}
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID": &dbtypes.AttributeValueMemberS{Value: "00002"},
							},
						}); err != nil {
							return err
						}
						return nil
					},
				}
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				// Process a first record.
				c := m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.MatchedBy(func(records []dbstreamstypes.Record) bool {
					// Validate the size of the record.
					if len(records) != 1 {
						return false
					}
					// Validate the record data.
					return validate(
						dbstreamstypes.Record{
							EventName: dbstreamstypes.OperationTypeModify,
							Dynamodb: &dbstreamstypes.StreamRecord{
								StreamViewType: dbstreamstypes.StreamViewTypeNewAndOldImages,
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
								},
								NewImage: map[string]dbstreamstypes.AttributeValue{
									"ID":  &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
									"Age": &dbstreamstypes.AttributeValueMemberN{Value: "1"},
								},
								OldImage: map[string]dbstreamstypes.AttributeValue{
									"ID":  &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
									"Age": &dbstreamstypes.AttributeValueMemberN{Value: "0"},
								},
							},
						},
						records[0],
					)
				})).Return(nil).Once()
				// Process a second record to break out of the loop by returning an error.
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.AnythingOfType("[]types.Record")).Return(errors.New("")).NotBefore(c).Once()
				return m
			},
		},
		{
			name: "Normal: process a single removed record",
			in: &in{
				triggers: func(id string) map[string][]string {
					return map[string][]string{
						s.tableName(id): {"FunctionName"},
					}
				},
			},
			out: &out{
				err: errors.New("failed to process some records"),
			},
			hooks: func(id string) *WorkerHooks {
				return &WorkerHooks{
					PreSetup: func(ctx context.Context) error {
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID": &dbtypes.AttributeValueMemberS{Value: "00001"},
							},
						}); err != nil {
							return err
						}
						return nil
					},
					PostSetup: func(ctx context.Context) error {
						if _, err := s.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
							TableName: StringPtr(s.tableName(id)),
							Key: map[string]dbtypes.AttributeValue{
								"ID": &dbtypes.AttributeValueMemberS{Value: "00001"},
							},
						}); err != nil {
							return err
						}
						if _, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
							TableName: StringPtr(s.tableName(id)),
							Item: map[string]dbtypes.AttributeValue{
								"ID": &dbtypes.AttributeValueMemberS{Value: "00002"},
							},
						}); err != nil {
							return err
						}
						return nil
					},
				}
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				// Process a first record.
				c := m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.MatchedBy(func(records []dbstreamstypes.Record) bool {
					// Validate the size of the record.
					if len(records) != 1 {
						return false
					}
					// Validate the record data.
					return validate(
						dbstreamstypes.Record{
							EventName: dbstreamstypes.OperationTypeRemove,
							Dynamodb: &dbstreamstypes.StreamRecord{
								StreamViewType: dbstreamstypes.StreamViewTypeNewAndOldImages,
								Keys: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
								},
								NewImage: (map[string]dbstreamstypes.AttributeValue)(nil),
								OldImage: map[string]dbstreamstypes.AttributeValue{
									"ID": &dbstreamstypes.AttributeValueMemberS{Value: "00001"},
								},
							},
						},
						records[0],
					)
				})).Return(nil).Once()
				// Process a second record to break out of the loop by returning an error.
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.AnythingOfType("[]types.Record")).Return(errors.New("")).NotBefore(c).Once()
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), s.execute(ctx, func(t *testing.T, id string) {
			w := NewWorker(&WorkerConfig{
				DB:        s.db,
				DBStreams: s.dbstreams,
				Processor: tc.mockProcessor(),
			}, WithHooks(tc.hooks(id)), WithBatchSize(1))
			err := w.Execute(ctx, tc.in.triggers(id))
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		}))
	}
}

func (s *WorkerIntegrationTestSuite) execute(ctx context.Context, do func(t *testing.T, id string)) func(t *testing.T) {
	return func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping test in short mode")
		}
		// Use a random ID as a table prefix for each test to allow parallel execution of test.
		id := RandomString(20)
		// Execute a setup function.
		if err := s.setup(ctx, id); err != nil {
			t.Fatalf("%+v", err)
		}
		// Execute a teardown function.
		defer func() {
			if err := s.teardown(ctx, id); err != nil {
				t.Fatalf("%+v", err)
			}
		}()
		do(t, id)
	}
}

func (s *WorkerIntegrationTestSuite) setup(ctx context.Context, id string) error {
	if _, err := s.db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: StringPtr(s.tableName(id)),
		KeySchema: []dbtypes.KeySchemaElement{
			{
				AttributeName: StringPtr("ID"),
				KeyType:       dbtypes.KeyTypeHash,
			},
		},
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{
				AttributeName: StringPtr("ID"),
				AttributeType: dbtypes.ScalarAttributeTypeS,
			},
		},
		ProvisionedThroughput: &dbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  Int64Ptr(1),
			WriteCapacityUnits: Int64Ptr(1),
		},
		StreamSpecification: &dbtypes.StreamSpecification{
			StreamEnabled:  BoolPtr(true),
			StreamViewType: dbtypes.StreamViewTypeNewAndOldImages,
		},
	}); err != nil {
		return errors.Wrap(err, "failed to execute DynamoDB#CreateTable")
	}
	return nil
}

func (s *WorkerIntegrationTestSuite) teardown(ctx context.Context, id string) error {
	if _, err := s.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: StringPtr(s.tableName(id)),
	}); err != nil {
		return errors.Wrap(err, "failed to execute DynamoDB#DeleteTable")
	}
	return nil
}

func (s *WorkerIntegrationTestSuite) tableName(id string) string {
	name := "Test"
	if id != "" {
		return id + "." + name
	}
	return name
}

func TestWorkerIntegrationTestSuite(t *testing.T) {
	endpoint := "http://dynamodb:8000"
	// Create a DB client.
	db, err := NewDBClient(context.TODO(), endpoint)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// Create a DB Streams client.
	dbstreams, err := NewDBStreamsClient(context.TODO(), endpoint)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	// Disable standard output as log output.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Execute.
	suite.Run(t, &WorkerIntegrationTestSuite{
		db:        db,
		dbstreams: dbstreams,
	})
}
