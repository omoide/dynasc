package dynasc

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	dbstreamstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LambdaProcessorTestSuite struct {
	suite.Suite
}

func (s *LambdaProcessorTestSuite) TestProcess() {
	// Define structures for input and output.
	type in struct {
		functionName string
		records      []dbstreamstypes.Record
	}
	type out struct {
		err error
	}
	tests := []struct {
		name             string
		in               *in
		out              *out
		mockLambdaClient func() LambdaClient
	}{
		{
			name: "Normal: a single record exists",
			in: &in{
				functionName: "FunctionName",
				records: []dbstreamstypes.Record{
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
								"Name": &dbstreamstypes.AttributeValueMemberS{
									Value: "NameNew",
								},
							},
							OldImage: map[string]dbstreamstypes.AttributeValue{
								"ID": &dbstreamstypes.AttributeValueMemberS{
									Value: "00001",
								},
								"Name": &dbstreamstypes.AttributeValueMemberS{
									Value: "NameOld",
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
			},
			out: &out{},
			mockLambdaClient: func() LambdaClient {
				m := NewMockLambdaClient()
				m.On("Invoke", context.Background(), &lambda.InvokeInput{
					FunctionName: StringPtr("FunctionName"),
					Payload: func() []byte {
						b, _ := json.Marshal(&events.DynamoDBEvent{
							Records: []events.DynamoDBEventRecord{
								{
									AWSRegion:    "ap-northeast-1",
									EventID:      "EventID",
									EventName:    string(events.DynamoDBOperationTypeInsert),
									EventSource:  "aws:dynamodb",
									EventVersion: "1",
									Change: events.DynamoDBStreamRecord{
										ApproximateCreationDateTime: events.SecondsEpochTime{
											Time: time.Time{},
										},
										Keys: map[string]events.DynamoDBAttributeValue{
											"ID": events.NewStringAttribute("00001"),
										},
										NewImage: map[string]events.DynamoDBAttributeValue{
											"ID":   events.NewStringAttribute("00001"),
											"Name": events.NewStringAttribute("NameNew"),
										},
										OldImage: map[string]events.DynamoDBAttributeValue{
											"ID":   events.NewStringAttribute("00001"),
											"Name": events.NewStringAttribute("NameOld"),
										},
										SequenceNumber: "000000000000000000000001",
										SizeBytes:      256,
										StreamViewType: string(events.DynamoDBStreamViewTypeNewAndOldImages),
									},
									UserIdentity: &events.DynamoDBUserIdentity{
										Type:        "Service",
										PrincipalID: "dynamodb.amazonaws.com",
									},
								},
							},
						})
						return b
					}(),
					InvocationType: lambdatypes.InvocationTypeRequestResponse,
				}, ([]func(*lambda.Options))(nil)).Return(nil, nil)
				return m
			},
		},
		{
			name: "Abnormal: LambdaClient#Invoke returns an error",
			in: &in{
				functionName: "FunctionName",
				records:      []dbstreamstypes.Record{},
			},
			out: &out{
				err: errors.Newf("failed to invoke the lambda function: %s", "FunctionName"),
			},
			mockLambdaClient: func() LambdaClient {
				m := NewMockLambdaClient()
				m.On("Invoke", context.Background(), &lambda.InvokeInput{
					FunctionName: StringPtr("FunctionName"),
					Payload: func() []byte {
						b, _ := json.Marshal(&events.DynamoDBEvent{
							Records: []events.DynamoDBEventRecord{},
						})
						return b
					}(),
					InvocationType: lambdatypes.InvocationTypeRequestResponse,
				}, ([]func(*lambda.Options))(nil)).Return(nil, errors.New(""))
				return m
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		ctx := context.Background()
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			err := NewLambdaProcessor(tc.mockLambdaClient()).Process(ctx, tc.in.functionName, tc.in.records)
			if tc.out.err != nil {
				assert.ErrorContains(t, err, tc.out.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLambdaProcessorTestSuite(t *testing.T) {
	// Execute.
	suite.Run(t, &LambdaProcessorTestSuite{})
}

type DynamoDBAttributeValuesTestSuite struct {
	suite.Suite
}

func (s *DynamoDBAttributeValuesTestSuite) TestToEventAttributeValues() {
	// Define structures for input and output.
	type in struct {
		attributeValues DynamoDBAttributeValues
	}
	type out struct {
		eventAttributeValues map[string]events.DynamoDBAttributeValue
	}
	// Define test cases.
	tests := []struct {
		name string
		in   *in
		out  *out
	}{
		{
			name: "Normal: an attribute of type string is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"S": &dbstreamstypes.AttributeValueMemberS{
						Value: "value",
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"S": events.NewStringAttribute("value"),
				},
			},
		},
		{
			name: "Normal: an attribute of type string set is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"SS": &dbstreamstypes.AttributeValueMemberSS{
						Value: []string{"value1", "value2"},
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"SS": events.NewStringSetAttribute([]string{"value1", "value2"}),
				},
			},
		},
		{
			name: "Normal: an attribute of type number is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"N": &dbstreamstypes.AttributeValueMemberN{
						Value: "1",
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"N": events.NewNumberAttribute("1"),
				},
			},
		},
		{
			name: "Normal: an attribute of type number set is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"NS": &dbstreamstypes.AttributeValueMemberNS{
						Value: []string{"1", "2"},
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"NS": events.NewNumberSetAttribute([]string{"1", "2"}),
				},
			},
		},
		{
			name: "Normal: an attribute of type binary is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"B": &dbstreamstypes.AttributeValueMemberB{
						Value: []byte("value"),
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"B": events.NewBinaryAttribute([]byte("value")),
				},
			},
		},
		{
			name: "Normal: an attribute of type binary set is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"BS": &dbstreamstypes.AttributeValueMemberBS{
						Value: [][]byte{[]byte("value1"), []byte("value2")},
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"BS": events.NewBinarySetAttribute([][]byte{[]byte("value1"), []byte("value2")}),
				},
			},
		},
		{
			name: "Normal: an attribute of type boolean is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"BOOL": &dbstreamstypes.AttributeValueMemberBOOL{
						Value: true,
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"BOOL": events.NewBooleanAttribute(true),
				},
			},
		},
		{
			name: "Normal: an attribute of type list is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"L": &dbstreamstypes.AttributeValueMemberL{
						Value: []dbstreamstypes.AttributeValue{
							&dbstreamstypes.AttributeValueMemberBOOL{
								Value: true,
							},
							&dbstreamstypes.AttributeValueMemberNULL{
								Value: true,
							},
						},
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"L": events.NewListAttribute([]events.DynamoDBAttributeValue{
						events.NewBooleanAttribute(true),
						events.NewNullAttribute(),
					}),
				},
			},
		},
		{
			name: "Normal: an attribute of type map is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"M": &dbstreamstypes.AttributeValueMemberM{
						Value: map[string]dbstreamstypes.AttributeValue{
							"B": &dbstreamstypes.AttributeValueMemberBOOL{
								Value: true,
							},
							"NULL": &dbstreamstypes.AttributeValueMemberNULL{
								Value: true,
							},
						},
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"M": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
						"B":    events.NewBooleanAttribute(true),
						"NULL": events.NewNullAttribute(),
					}),
				},
			},
		},
		{
			name: "Normal: an attribute of type null is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"NULL": &dbstreamstypes.AttributeValueMemberNULL{
						Value: true,
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"NULL": events.NewNullAttribute(),
				},
			},
		},
		{
			name: "Normal: an unexpected attribute is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"UNEXPECTED": struct {
						dbstreamstypes.AttributeValue
					}{},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"UNEXPECTED": events.NewNullAttribute(),
				},
			},
		},
		{
			name: "Normal: no attribute is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{},
			},
		},
		{
			name: "Normal: every attribute is contained",
			in: &in{
				attributeValues: map[string]dbstreamstypes.AttributeValue{
					"S": &dbstreamstypes.AttributeValueMemberS{
						Value: "value",
					},
					"SS": &dbstreamstypes.AttributeValueMemberSS{
						Value: []string{"value1", "value2"},
					},
					"N": &dbstreamstypes.AttributeValueMemberN{
						Value: "1",
					},
					"NS": &dbstreamstypes.AttributeValueMemberNS{
						Value: []string{"1", "2"},
					},
					"B": &dbstreamstypes.AttributeValueMemberB{
						Value: []byte("value"),
					},
					"BS": &dbstreamstypes.AttributeValueMemberBS{
						Value: [][]byte{[]byte("value1"), []byte("value2")},
					},
					"BOOL": &dbstreamstypes.AttributeValueMemberBOOL{
						Value: true,
					},
					"L": &dbstreamstypes.AttributeValueMemberL{
						Value: []dbstreamstypes.AttributeValue{
							&dbstreamstypes.AttributeValueMemberBOOL{
								Value: true,
							},
							&dbstreamstypes.AttributeValueMemberNULL{
								Value: true,
							},
						},
					},
					"M": &dbstreamstypes.AttributeValueMemberM{
						Value: map[string]dbstreamstypes.AttributeValue{
							"B": &dbstreamstypes.AttributeValueMemberBOOL{
								Value: true,
							},
							"NULL": &dbstreamstypes.AttributeValueMemberNULL{
								Value: true,
							},
						},
					},
					"NULL": &dbstreamstypes.AttributeValueMemberNULL{
						Value: true,
					},
				},
			},
			out: &out{
				eventAttributeValues: map[string]events.DynamoDBAttributeValue{
					"S":    events.NewStringAttribute("value"),
					"SS":   events.NewStringSetAttribute([]string{"value1", "value2"}),
					"N":    events.NewNumberAttribute("1"),
					"NS":   events.NewNumberSetAttribute([]string{"1", "2"}),
					"B":    events.NewBinaryAttribute([]byte("value")),
					"BS":   events.NewBinarySetAttribute([][]byte{[]byte("value1"), []byte("value2")}),
					"BOOL": events.NewBooleanAttribute(true),
					"L": events.NewListAttribute([]events.DynamoDBAttributeValue{
						events.NewBooleanAttribute(true),
						events.NewNullAttribute(),
					}),
					"M": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
						"B":    events.NewBooleanAttribute(true),
						"NULL": events.NewNullAttribute(),
					}),
					"NULL": events.NewNullAttribute(),
				},
			},
		},
	}
	// Execute.
	for i, tc := range tests {
		s.T().Run(fmt.Sprintf("#%02d:%s", i+1, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.out.eventAttributeValues, tc.in.attributeValues.ToEventAttributeValues())
		})
	}
}

func TestDynamoDBAttributeValuesTestSuite(t *testing.T) {
	// Execute.
	suite.Run(t, &DynamoDBAttributeValuesTestSuite{})
}
