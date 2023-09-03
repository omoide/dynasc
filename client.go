package dynasc

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/cockroachdb/errors"
)

// DBClient represents interfaces of Amazon DynamoDB client.
type DBClient interface {
	BatchExecuteStatement(context.Context, *dynamodb.BatchExecuteStatementInput, ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error)
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	CreateBackup(context.Context, *dynamodb.CreateBackupInput, ...func(*dynamodb.Options)) (*dynamodb.CreateBackupOutput, error)
	CreateGlobalTable(context.Context, *dynamodb.CreateGlobalTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateGlobalTableOutput, error)
	CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DeleteBackup(context.Context, *dynamodb.DeleteBackupInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteBackupOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	DeleteTable(context.Context, *dynamodb.DeleteTableInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error)
	DescribeBackup(context.Context, *dynamodb.DescribeBackupInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeBackupOutput, error)
	DescribeContinuousBackups(context.Context, *dynamodb.DescribeContinuousBackupsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error)
	DescribeContributorInsights(context.Context, *dynamodb.DescribeContributorInsightsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeContributorInsightsOutput, error)
	DescribeEndpoints(context.Context, *dynamodb.DescribeEndpointsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeEndpointsOutput, error)
	DescribeExport(context.Context, *dynamodb.DescribeExportInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeExportOutput, error)
	DescribeGlobalTable(context.Context, *dynamodb.DescribeGlobalTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableOutput, error)
	DescribeGlobalTableSettings(context.Context, *dynamodb.DescribeGlobalTableSettingsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableSettingsOutput, error)
	DescribeImport(context.Context, *dynamodb.DescribeImportInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeImportOutput, error)
	DescribeKinesisStreamingDestination(context.Context, *dynamodb.DescribeKinesisStreamingDestinationInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error)
	DescribeLimits(context.Context, *dynamodb.DescribeLimitsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeLimitsOutput, error)
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	DescribeTableReplicaAutoScaling(context.Context, *dynamodb.DescribeTableReplicaAutoScalingInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error)
	DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	DisableKinesisStreamingDestination(context.Context, *dynamodb.DisableKinesisStreamingDestinationInput, ...func(*dynamodb.Options)) (*dynamodb.DisableKinesisStreamingDestinationOutput, error)
	EnableKinesisStreamingDestination(context.Context, *dynamodb.EnableKinesisStreamingDestinationInput, ...func(*dynamodb.Options)) (*dynamodb.EnableKinesisStreamingDestinationOutput, error)
	ExecuteStatement(context.Context, *dynamodb.ExecuteStatementInput, ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error)
	ExecuteTransaction(context.Context, *dynamodb.ExecuteTransactionInput, ...func(*dynamodb.Options)) (*dynamodb.ExecuteTransactionOutput, error)
	ExportTableToPointInTime(context.Context, *dynamodb.ExportTableToPointInTimeInput, ...func(*dynamodb.Options)) (*dynamodb.ExportTableToPointInTimeOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	ImportTable(context.Context, *dynamodb.ImportTableInput, ...func(*dynamodb.Options)) (*dynamodb.ImportTableOutput, error)
	ListBackups(context.Context, *dynamodb.ListBackupsInput, ...func(*dynamodb.Options)) (*dynamodb.ListBackupsOutput, error)
	ListContributorInsights(context.Context, *dynamodb.ListContributorInsightsInput, ...func(*dynamodb.Options)) (*dynamodb.ListContributorInsightsOutput, error)
	ListExports(context.Context, *dynamodb.ListExportsInput, ...func(*dynamodb.Options)) (*dynamodb.ListExportsOutput, error)
	ListGlobalTables(context.Context, *dynamodb.ListGlobalTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListGlobalTablesOutput, error)
	ListImports(context.Context, *dynamodb.ListImportsInput, ...func(*dynamodb.Options)) (*dynamodb.ListImportsOutput, error)
	ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
	ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	RestoreTableFromBackup(context.Context, *dynamodb.RestoreTableFromBackupInput, ...func(*dynamodb.Options)) (*dynamodb.RestoreTableFromBackupOutput, error)
	RestoreTableToPointInTime(context.Context, *dynamodb.RestoreTableToPointInTimeInput, ...func(*dynamodb.Options)) (*dynamodb.RestoreTableToPointInTimeOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	TagResource(context.Context, *dynamodb.TagResourceInput, ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error)
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	UntagResource(context.Context, *dynamodb.UntagResourceInput, ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error)
	UpdateContinuousBackups(context.Context, *dynamodb.UpdateContinuousBackupsInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error)
	UpdateContributorInsights(context.Context, *dynamodb.UpdateContributorInsightsInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateContributorInsightsOutput, error)
	UpdateGlobalTable(context.Context, *dynamodb.UpdateGlobalTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableOutput, error)
	UpdateGlobalTableSettings(context.Context, *dynamodb.UpdateGlobalTableSettingsInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableSettingsOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
	UpdateTableReplicaAutoScaling(context.Context, *dynamodb.UpdateTableReplicaAutoScalingInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error)
	UpdateTimeToLive(context.Context, *dynamodb.UpdateTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
}

// NewDBClient returns a client of Amazon DynamoDB.
func NewDBClient(ctx context.Context, endpoint string) (*dynamodb.Client, error) {
	config, err := config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(endpointResolver(endpoint)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load the AWS default configuration")
	}
	return dynamodb.NewFromConfig(config), nil
}

type DBStreamsClient interface {
	DescribeStream(context.Context, *dynamodbstreams.DescribeStreamInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error)
	GetRecords(context.Context, *dynamodbstreams.GetRecordsInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error)
	GetShardIterator(context.Context, *dynamodbstreams.GetShardIteratorInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error)
	ListStreams(context.Context, *dynamodbstreams.ListStreamsInput, ...func(*dynamodbstreams.Options)) (*dynamodbstreams.ListStreamsOutput, error)
}

// NewDBStreamsClient returns a client of Amazon DynamoDB Streams.
func NewDBStreamsClient(ctx context.Context, endpoint string) (*dynamodbstreams.Client, error) {
	config, err := config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(endpointResolver(endpoint)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load the AWS default configuration")
	}
	return dynamodbstreams.NewFromConfig(config), nil
}

// LambdaClient represents interfaces of AWS Lambda client.
type LambdaClient interface {
	AddLayerVersionPermission(context.Context, *lambda.AddLayerVersionPermissionInput, ...func(*lambda.Options)) (*lambda.AddLayerVersionPermissionOutput, error)
	AddPermission(context.Context, *lambda.AddPermissionInput, ...func(*lambda.Options)) (*lambda.AddPermissionOutput, error)
	CreateAlias(context.Context, *lambda.CreateAliasInput, ...func(*lambda.Options)) (*lambda.CreateAliasOutput, error)
	CreateCodeSigningConfig(context.Context, *lambda.CreateCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.CreateCodeSigningConfigOutput, error)
	CreateEventSourceMapping(context.Context, *lambda.CreateEventSourceMappingInput, ...func(*lambda.Options)) (*lambda.CreateEventSourceMappingOutput, error)
	CreateFunction(context.Context, *lambda.CreateFunctionInput, ...func(*lambda.Options)) (*lambda.CreateFunctionOutput, error)
	CreateFunctionUrlConfig(context.Context, *lambda.CreateFunctionUrlConfigInput, ...func(*lambda.Options)) (*lambda.CreateFunctionUrlConfigOutput, error)
	DeleteAlias(context.Context, *lambda.DeleteAliasInput, ...func(*lambda.Options)) (*lambda.DeleteAliasOutput, error)
	DeleteCodeSigningConfig(context.Context, *lambda.DeleteCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.DeleteCodeSigningConfigOutput, error)
	DeleteEventSourceMapping(context.Context, *lambda.DeleteEventSourceMappingInput, ...func(*lambda.Options)) (*lambda.DeleteEventSourceMappingOutput, error)
	DeleteFunction(context.Context, *lambda.DeleteFunctionInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionOutput, error)
	DeleteFunctionCodeSigningConfig(context.Context, *lambda.DeleteFunctionCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionCodeSigningConfigOutput, error)
	DeleteFunctionConcurrency(context.Context, *lambda.DeleteFunctionConcurrencyInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionConcurrencyOutput, error)
	DeleteFunctionEventInvokeConfig(context.Context, *lambda.DeleteFunctionEventInvokeConfigInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionEventInvokeConfigOutput, error)
	DeleteFunctionUrlConfig(context.Context, *lambda.DeleteFunctionUrlConfigInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionUrlConfigOutput, error)
	DeleteLayerVersion(context.Context, *lambda.DeleteLayerVersionInput, ...func(*lambda.Options)) (*lambda.DeleteLayerVersionOutput, error)
	DeleteProvisionedConcurrencyConfig(context.Context, *lambda.DeleteProvisionedConcurrencyConfigInput, ...func(*lambda.Options)) (*lambda.DeleteProvisionedConcurrencyConfigOutput, error)
	GetAccountSettings(context.Context, *lambda.GetAccountSettingsInput, ...func(*lambda.Options)) (*lambda.GetAccountSettingsOutput, error)
	GetAlias(context.Context, *lambda.GetAliasInput, ...func(*lambda.Options)) (*lambda.GetAliasOutput, error)
	GetCodeSigningConfig(context.Context, *lambda.GetCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.GetCodeSigningConfigOutput, error)
	GetEventSourceMapping(context.Context, *lambda.GetEventSourceMappingInput, ...func(*lambda.Options)) (*lambda.GetEventSourceMappingOutput, error)
	GetFunction(context.Context, *lambda.GetFunctionInput, ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error)
	GetFunctionCodeSigningConfig(context.Context, *lambda.GetFunctionCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.GetFunctionCodeSigningConfigOutput, error)
	GetFunctionConcurrency(context.Context, *lambda.GetFunctionConcurrencyInput, ...func(*lambda.Options)) (*lambda.GetFunctionConcurrencyOutput, error)
	GetFunctionConfiguration(context.Context, *lambda.GetFunctionConfigurationInput, ...func(*lambda.Options)) (*lambda.GetFunctionConfigurationOutput, error)
	GetFunctionEventInvokeConfig(context.Context, *lambda.GetFunctionEventInvokeConfigInput, ...func(*lambda.Options)) (*lambda.GetFunctionEventInvokeConfigOutput, error)
	GetFunctionUrlConfig(context.Context, *lambda.GetFunctionUrlConfigInput, ...func(*lambda.Options)) (*lambda.GetFunctionUrlConfigOutput, error)
	GetLayerVersion(context.Context, *lambda.GetLayerVersionInput, ...func(*lambda.Options)) (*lambda.GetLayerVersionOutput, error)
	GetLayerVersionByArn(context.Context, *lambda.GetLayerVersionByArnInput, ...func(*lambda.Options)) (*lambda.GetLayerVersionByArnOutput, error)
	GetLayerVersionPolicy(context.Context, *lambda.GetLayerVersionPolicyInput, ...func(*lambda.Options)) (*lambda.GetLayerVersionPolicyOutput, error)
	GetPolicy(context.Context, *lambda.GetPolicyInput, ...func(*lambda.Options)) (*lambda.GetPolicyOutput, error)
	GetProvisionedConcurrencyConfig(context.Context, *lambda.GetProvisionedConcurrencyConfigInput, ...func(*lambda.Options)) (*lambda.GetProvisionedConcurrencyConfigOutput, error)
	GetRuntimeManagementConfig(context.Context, *lambda.GetRuntimeManagementConfigInput, ...func(*lambda.Options)) (*lambda.GetRuntimeManagementConfigOutput, error)
	Invoke(context.Context, *lambda.InvokeInput, ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
	InvokeAsync(context.Context, *lambda.InvokeAsyncInput, ...func(*lambda.Options)) (*lambda.InvokeAsyncOutput, error)
	InvokeWithResponseStream(context.Context, *lambda.InvokeWithResponseStreamInput, ...func(*lambda.Options)) (*lambda.InvokeWithResponseStreamOutput, error)
	ListAliases(context.Context, *lambda.ListAliasesInput, ...func(*lambda.Options)) (*lambda.ListAliasesOutput, error)
	ListCodeSigningConfigs(context.Context, *lambda.ListCodeSigningConfigsInput, ...func(*lambda.Options)) (*lambda.ListCodeSigningConfigsOutput, error)
	ListEventSourceMappings(context.Context, *lambda.ListEventSourceMappingsInput, ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error)
	ListFunctionEventInvokeConfigs(context.Context, *lambda.ListFunctionEventInvokeConfigsInput, ...func(*lambda.Options)) (*lambda.ListFunctionEventInvokeConfigsOutput, error)
	ListFunctionUrlConfigs(context.Context, *lambda.ListFunctionUrlConfigsInput, ...func(*lambda.Options)) (*lambda.ListFunctionUrlConfigsOutput, error)
	ListFunctions(context.Context, *lambda.ListFunctionsInput, ...func(*lambda.Options)) (*lambda.ListFunctionsOutput, error)
	ListFunctionsByCodeSigningConfig(context.Context, *lambda.ListFunctionsByCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.ListFunctionsByCodeSigningConfigOutput, error)
	ListLayerVersions(context.Context, *lambda.ListLayerVersionsInput, ...func(*lambda.Options)) (*lambda.ListLayerVersionsOutput, error)
	ListLayers(context.Context, *lambda.ListLayersInput, ...func(*lambda.Options)) (*lambda.ListLayersOutput, error)
	ListProvisionedConcurrencyConfigs(context.Context, *lambda.ListProvisionedConcurrencyConfigsInput, ...func(*lambda.Options)) (*lambda.ListProvisionedConcurrencyConfigsOutput, error)
	ListTags(context.Context, *lambda.ListTagsInput, ...func(*lambda.Options)) (*lambda.ListTagsOutput, error)
	ListVersionsByFunction(context.Context, *lambda.ListVersionsByFunctionInput, ...func(*lambda.Options)) (*lambda.ListVersionsByFunctionOutput, error)
	PublishLayerVersion(context.Context, *lambda.PublishLayerVersionInput, ...func(*lambda.Options)) (*lambda.PublishLayerVersionOutput, error)
	PublishVersion(context.Context, *lambda.PublishVersionInput, ...func(*lambda.Options)) (*lambda.PublishVersionOutput, error)
	PutFunctionCodeSigningConfig(context.Context, *lambda.PutFunctionCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.PutFunctionCodeSigningConfigOutput, error)
	PutFunctionConcurrency(context.Context, *lambda.PutFunctionConcurrencyInput, ...func(*lambda.Options)) (*lambda.PutFunctionConcurrencyOutput, error)
	PutFunctionEventInvokeConfig(context.Context, *lambda.PutFunctionEventInvokeConfigInput, ...func(*lambda.Options)) (*lambda.PutFunctionEventInvokeConfigOutput, error)
	PutProvisionedConcurrencyConfig(context.Context, *lambda.PutProvisionedConcurrencyConfigInput, ...func(*lambda.Options)) (*lambda.PutProvisionedConcurrencyConfigOutput, error)
	PutRuntimeManagementConfig(context.Context, *lambda.PutRuntimeManagementConfigInput, ...func(*lambda.Options)) (*lambda.PutRuntimeManagementConfigOutput, error)
	RemoveLayerVersionPermission(context.Context, *lambda.RemoveLayerVersionPermissionInput, ...func(*lambda.Options)) (*lambda.RemoveLayerVersionPermissionOutput, error)
	RemovePermission(context.Context, *lambda.RemovePermissionInput, ...func(*lambda.Options)) (*lambda.RemovePermissionOutput, error)
	TagResource(context.Context, *lambda.TagResourceInput, ...func(*lambda.Options)) (*lambda.TagResourceOutput, error)
	UntagResource(context.Context, *lambda.UntagResourceInput, ...func(*lambda.Options)) (*lambda.UntagResourceOutput, error)
	UpdateAlias(context.Context, *lambda.UpdateAliasInput, ...func(*lambda.Options)) (*lambda.UpdateAliasOutput, error)
	UpdateCodeSigningConfig(context.Context, *lambda.UpdateCodeSigningConfigInput, ...func(*lambda.Options)) (*lambda.UpdateCodeSigningConfigOutput, error)
	UpdateEventSourceMapping(context.Context, *lambda.UpdateEventSourceMappingInput, ...func(*lambda.Options)) (*lambda.UpdateEventSourceMappingOutput, error)
	UpdateFunctionCode(context.Context, *lambda.UpdateFunctionCodeInput, ...func(*lambda.Options)) (*lambda.UpdateFunctionCodeOutput, error)
	UpdateFunctionConfiguration(context.Context, *lambda.UpdateFunctionConfigurationInput, ...func(*lambda.Options)) (*lambda.UpdateFunctionConfigurationOutput, error)
	UpdateFunctionEventInvokeConfig(context.Context, *lambda.UpdateFunctionEventInvokeConfigInput, ...func(*lambda.Options)) (*lambda.UpdateFunctionEventInvokeConfigOutput, error)
	UpdateFunctionUrlConfig(context.Context, *lambda.UpdateFunctionUrlConfigInput, ...func(*lambda.Options)) (*lambda.UpdateFunctionUrlConfigOutput, error)
}

// NewLambdaClient returns a client of AWS Lambda.
func NewLambdaClient(ctx context.Context, endpoint string) (*lambda.Client, error) {
	config, err := config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(endpointResolver(endpoint)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load the AWS default configuration")
	}
	return lambda.NewFromConfig(config), nil
}

func endpointResolver(endpoint string) aws.EndpointResolverWithOptions {
	return aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: endpoint,
		}, nil
	})
}
