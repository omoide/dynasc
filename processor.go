package dynasc

import (
	"context"

	dbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

// Processor represents a processor that processes records in DynamoDB Streams.
type Processor interface {
	Process(ctx context.Context, functionName string, records []dbstreamstypes.Record) error
}
