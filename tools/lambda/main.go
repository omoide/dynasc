package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	lambda.Start(func(ctx context.Context, event events.DynamoDBEvent) error {
		fmt.Printf("%+v\n", event)
		return nil
	})
}
