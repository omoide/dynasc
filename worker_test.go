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
	}
	type out struct {
		err error
	}
	// Define test cases.
	tests := []struct {
		name                string
		in                  *in
		out                 *out
		triggers            map[string][]string
		mockDBClient        func() DBClient
		mockDBStreamsClient func() DBStreamsClient
		mockProcessor       func() Processor
	}{
		{
			name: "Normal: a single trigger exists",
			in:   &in{},
			out:  &out{},
			triggers: map[string][]string{
				"TableName": {"FunctionName"},
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
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
			name:     "Normal: no trigger exists",
			in:       &in{},
			out:      &out{},
			triggers: map[string][]string{},
			mockDBClient: func() DBClient {
				m := NewMockDBClient()
				return m
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
			name: "Abnormal: Worker#streamARN returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to generate triggers with stream ARN as the key"),
			},
			triggers: map[string][]string{
				"TableName": {"FunctionName"},
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
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: Worker#executeStreamWorker returns an error",
			in:   &in{},
			out: &out{
				err: errors.New("failed to process streams"),
			},
			triggers: map[string][]string{
				"TableName": {"FunctionName"},
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
				DB:        tc.mockDBClient(),
				DBStreams: tc.mockDBStreamsClient(),
				Processor: tc.mockProcessor(),
				Triggers:  tc.triggers,
			})
			err := w.execute(ctx)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func (s *WorkerTestSuite) TestExecuteStreamWorker() {
	// Define structures for input and output.
	type in struct {
		streamARN     string
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
			name: "Normal: a single shard exists",
			in: &in{
				streamARN:     "arn:aws:dynamodb:000000000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
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
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", mock.AnythingOfType("*context.cancelCtx"), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
			name: "Normal: no shard exists",
			in: &in{
				streamARN:     "arn:aws:dynamodb:000000000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.DescribeStreamOutput{
					StreamDescription: &dbstreamstypes.StreamDescription{
						Shards:               []dbstreamstypes.Shard{},
						LastEvaluatedShardId: nil,
					},
				}, nil)
				return m
			},
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: Worker#shards returns an error",
			in: &in{
				streamARN:     "arn:aws:dynamodb:000000000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.Newf("failed to get the shards of the specified stream: %s", "arn:aws:dynamodb:000000000000"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("DescribeStream", context.Background(), &dynamodbstreams.DescribeStreamInput{
					StreamArn:             StringPtr("arn:aws:dynamodb:000000000000"),
					ExclusiveStartShardId: nil,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(nil, errors.New(""))
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
				streamARN:     "arn:aws:dynamodb:000000000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to process shards"),
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
				DBStreams: tc.mockDBStreamsClient(),
				Processor: tc.mockProcessor(),
			})
			err := w.executeStreamWorker(ctx, tc.in.streamARN, tc.in.functionNames)
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
		streamARN     string
		shardID       string
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
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName1", "FunctionName2"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
					Limit:         Int32Ptr(100),
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetRecordsOutput{
					NextShardIterator: StringPtr("shardIterator-00001"),
					Records:           []dbstreamstypes.Record{},
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00001"),
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
			name: "Abnormal: DynamoDBStreams#GetShardIterator returns an error",
			in: &in{
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to execute DynamoDBStreams#GetShardIterators"),
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
			mockProcessor: func() Processor {
				m := NewMockProcessor()
				return m
			},
		},
		{
			name: "Abnormal: DynamoDBStreams#GetRecords returns an error",
			in: &in{
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to execute DynamoDBStreams#GetRecords"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
				streamARN:     "arn:aws:dynamodb:000000000000",
				shardID:       "shardId-00000000000000000000-00000000",
				functionNames: []string{"FunctionName"},
			},
			out: &out{
				err: errors.New("failed to process records"),
			},
			mockDBStreamsClient: func() DBStreamsClient {
				m := NewMockDBStreamsClient()
				m.On("GetShardIterator", context.Background(), &dynamodbstreams.GetShardIteratorInput{
					StreamArn:         StringPtr("arn:aws:dynamodb:000000000000"),
					ShardId:           StringPtr("shardId-00000000000000000000-00000000"),
					ShardIteratorType: dbstreamstypes.ShardIteratorTypeLatest,
				}, ([]func(*dynamodbstreams.Options))(nil)).Return(&dynamodbstreams.GetShardIteratorOutput{
					ShardIterator: StringPtr("shardIterator-00000"),
				}, nil)
				m.On("GetRecords", context.Background(), &dynamodbstreams.GetRecordsInput{
					ShardIterator: StringPtr("shardIterator-00000"),
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
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			w := NewWorker(&WorkerConfig{
				DB:        nil,
				DBStreams: tc.mockDBStreamsClient(),
				Processor: tc.mockProcessor(),
			})
			err := w.executeShardWorker(ctx, tc.in.streamARN, tc.in.shardID, tc.in.functionNames)
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
			name: "Normal: a last evaluated shard ID does not exist",
			in: &in{
				streamARN: "arn:aws:dynamodb:000000000000",
			},
			out: &out{
				shards: []dbstreamstypes.Shard{
					{
						ShardId: StringPtr("shardId-00000000000000000000-00000000"),
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
					},
					{
						ShardId: StringPtr("shardId-00000000000000000000-00000001"),
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
		triggers      func(id string) map[string][]string
		hooks         func(id string) *WorkerHooks
		mockProcessor func() Processor
	}{
		{
			name: "Normal: process a single inserted record",
			in:   &in{},
			out: &out{
				err: errors.New("failed to process streams"),
			},
			triggers: func(id string) map[string][]string {
				return map[string][]string{
					s.tableName(id): {"FunctionName"},
				}
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
					act := records[0]
					exp := dbstreamstypes.Record{
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
					}
					return validate(exp, act)
				})).Return(nil).Once()
				// Process a second record to break out of the loop by returning an error.
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.AnythingOfType("[]types.Record")).Return(errors.New("")).NotBefore(c).Once()
				return m
			},
		},
		{
			name: "Normal: process a single modified record",
			in:   &in{},
			out: &out{
				err: errors.New("failed to process streams"),
			},
			triggers: func(id string) map[string][]string {
				return map[string][]string{
					s.tableName(id): {"FunctionName"},
				}
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
					act := records[0]
					exp := dbstreamstypes.Record{
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
					}
					return validate(exp, act)
				})).Return(nil).Once()
				// Process a second record to break out of the loop by returning an error.
				m.On("Process", mock.AnythingOfType("*context.cancelCtx"), "FunctionName", mock.AnythingOfType("[]types.Record")).Return(errors.New("")).NotBefore(c).Once()
				return m
			},
		},
		{
			name: "Normal: process a single removed record",
			in:   &in{},
			out: &out{
				err: errors.New("failed to process streams"),
			},
			triggers: func(id string) map[string][]string {
				return map[string][]string{
					s.tableName(id): {"FunctionName"},
				}
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
					act := records[0]
					exp := dbstreamstypes.Record{
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
					}
					return validate(exp, act)
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
				Triggers:  tc.triggers(id),
			}, WithHooks(tc.hooks(id)), WithBatchSize(1))
			err := w.Execute(ctx)
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
	name := "TestTable"
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
