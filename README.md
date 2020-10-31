# go-dynamodb-stream-subscriber

## Usage
Go channel for streaming Dynamodb Updates

```go
package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/urakozz/go-dynamodb-stream-subscriber/stream"
)

func main() {
	cfg := aws.NewConfig().WithRegion("eu-west-1")
	sess := session.New()
	streamSvc := dynamodbstreams.New(sess, cfg)
	dynamoSvc := dynamodb.New(sess, cfg)
	table := "tableName"

	streamSubscriber := stream.NewStreamSubscriber(dynamoSvc, streamSvc, table)
	ch, errCh := streamSubscriber.GetStreamDataAsync()

	go func(errCh <-chan error) {
		for err := range errCh {
			fmt.Println("Stream Subscriber error: ", err)
		}
	}(errCh)

	for record := range ch {
		fmt.Println("from channel:", record)
	}
}
```

## Deployment

If using this in actual deployment. There is a throttle on the shard implementation on AWS. That means that if you have a large deployment and have multiple calls towards the same shard AWS may very well throttle the calls resulting in ProvisionedThroughputExceededException and triggering a back-off.

The solution (before actually doing this) may be to have a 1:1 connection of applicatoin and shard to guarantee not hitting the limit.

