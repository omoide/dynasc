package dynasc

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	dbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/cockroachdb/errors"
)

// LambdaProcessor represents a processor that processes records in DynamoDB Streams by invoking Lambda functions.
type LambdaProcessor struct {
	client LambdaClient
}

// NewLambdaProcessor returns an instance of the lambda processor.
func NewLambdaProcessor(client LambdaClient) *LambdaProcessor {
	return &LambdaProcessor{
		client: client,
	}
}

// Process processes records in DynamoDB Streams.
func (p *LambdaProcessor) Process(ctx context.Context, functionName string, records []dbstreamstypes.Record) error {
	eventRecords := []events.DynamoDBEventRecord{}
	for _, record := range records {
		eventRecords = append(eventRecords, events.DynamoDBEventRecord{
			AWSRegion:    *record.AwsRegion,
			EventID:      *record.EventID,
			EventName:    string(record.EventName),
			EventSource:  *record.EventSource,
			EventVersion: *record.EventVersion,
			Change: events.DynamoDBStreamRecord{
				ApproximateCreationDateTime: events.SecondsEpochTime{
					Time: *record.Dynamodb.ApproximateCreationDateTime,
				},
				Keys:           DynamoDBAttributeValues(record.Dynamodb.Keys).ToEventAttributeValues(),
				NewImage:       DynamoDBAttributeValues(record.Dynamodb.NewImage).ToEventAttributeValues(),
				OldImage:       DynamoDBAttributeValues(record.Dynamodb.OldImage).ToEventAttributeValues(),
				SequenceNumber: *record.Dynamodb.SequenceNumber,
				SizeBytes:      *record.Dynamodb.SizeBytes,
				StreamViewType: string(record.Dynamodb.StreamViewType),
			},
			UserIdentity: &events.DynamoDBUserIdentity{
				Type:        *record.UserIdentity.Type,
				PrincipalID: *record.UserIdentity.PrincipalId,
			},
		})
	}
	event := &events.DynamoDBEvent{
		Records: eventRecords,
	}
	b, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal the event record")
	}
	if _, err := p.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   StringPtr(functionName),
		Payload:        b,
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
	}); err != nil {
		return errors.Wrapf(err, "failed to invoke the lambda function: %s", functionName)
	}
	return nil
}

// DynamoDBAttributeValues represents a set of DynamoDBStreams AttributeValues.
type DynamoDBAttributeValues map[string]dbstreamstypes.AttributeValue

// ToEventAttributeValues converts a set of DynamoDBStreams AttributeValues to a set of Lambda Event DynamoDBAttributeValues.
func (dav DynamoDBAttributeValues) ToEventAttributeValues() map[string]events.DynamoDBAttributeValue {
	// Define the function to convert a DynamoDBStreams AttributeValue to a Lambda Event DynamoDBAttributeValue.
	var toEventAttributeValue func(av dbstreamstypes.AttributeValue) events.DynamoDBAttributeValue
	toEventAttributeValue = func(av dbstreamstypes.AttributeValue) events.DynamoDBAttributeValue {
		switch av := av.(type) {
		case *dbstreamstypes.AttributeValueMemberS:
			return events.NewStringAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberSS:
			return events.NewStringSetAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberN:
			return events.NewNumberAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberNS:
			return events.NewNumberSetAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberB:
			return events.NewBinaryAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberBS:
			return events.NewBinarySetAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberBOOL:
			return events.NewBooleanAttribute(av.Value)
		case *dbstreamstypes.AttributeValueMemberL:
			items := []events.DynamoDBAttributeValue{}
			for _, v := range av.Value {
				items = append(items, toEventAttributeValue(v))
			}
			return events.NewListAttribute(items)
		case *dbstreamstypes.AttributeValueMemberM:
			items := map[string]events.DynamoDBAttributeValue{}
			for k, v := range av.Value {
				items[k] = toEventAttributeValue(v)
			}
			return events.NewMapAttribute(items)
		case *dbstreamstypes.AttributeValueMemberNULL:
			return events.NewNullAttribute()
		}
		return events.NewNullAttribute()
	}
	// Convert.
	items := map[string]events.DynamoDBAttributeValue{}
	for k, v := range map[string]dbstreamstypes.AttributeValue(dav) {
		items[k] = toEventAttributeValue(v)
	}
	return items
}
