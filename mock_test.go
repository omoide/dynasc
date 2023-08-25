package dynasc

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/mock"
)

type MockDBClient struct {
	DBClient
	mock.Mock
}

func NewMockDBClient() *MockDBClient {
	return &MockDBClient{}
}

func (c *MockDBClient) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	result := c.Called(ctx, params, optFns)
	if v, ok := result.Get(0).(*dynamodb.DescribeTableOutput); ok {
		return v, result.Error(1)
	}
	return nil, result.Error(1)
}

type MockDBStreamsClient struct {
	DBStreamsClient
	mock.Mock
}

func NewMockDBStreamsClient() *MockDBStreamsClient {
	return &MockDBStreamsClient{}
}

func (c *MockDBStreamsClient) GetShardIterator(ctx context.Context, params *dynamodbstreams.GetShardIteratorInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error) {
	result := c.Called(ctx, params, optFns)
	if v, ok := result.Get(0).(*dynamodbstreams.GetShardIteratorOutput); ok {
		return v, result.Error(1)
	}
	return nil, result.Error(1)
}

func (c *MockDBStreamsClient) GetRecords(ctx context.Context, params *dynamodbstreams.GetRecordsInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	result := c.Called(ctx, params, optFns)
	if v, ok := result.Get(0).(*dynamodbstreams.GetRecordsOutput); ok {
		return v, result.Error(1)
	}
	return nil, result.Error(1)
}

func (c *MockDBStreamsClient) DescribeStream(ctx context.Context, params *dynamodbstreams.DescribeStreamInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	result := c.Called(ctx, params, optFns)
	if v, ok := result.Get(0).(*dynamodbstreams.DescribeStreamOutput); ok {
		return v, result.Error(1)
	}
	return nil, result.Error(1)
}

type MockLambdaClient struct {
	LambdaClient
	mock.Mock
}

func NewMockLambdaClient() *MockLambdaClient {
	return &MockLambdaClient{}
}

func (c *MockLambdaClient) Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	result := c.Called(ctx, params, optFns)
	if v, ok := result.Get(0).(*lambda.InvokeOutput); ok {
		return v, result.Error(1)
	}
	return nil, result.Error(1)
}

type MockProcessor struct {
	mock.Mock
}

func NewMockProcessor() *MockProcessor {
	return &MockProcessor{}
}

func (p *MockProcessor) Process(ctx context.Context, functionName string, records []dbstreamstypes.Record) error {
	result := p.Called(ctx, functionName, records)
	return result.Error(0)
}
