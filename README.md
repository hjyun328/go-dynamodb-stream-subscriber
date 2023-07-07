# go-dynamodb-stream-subscriber

## Usage
Go channel for streaming Dynamodb Updates

```go
package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"time"
)

func main() {
	region := "ap-northeast-2"
	table := "test"

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		panic(err)
	}

	streamCfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		panic(err)
	}

	subscriber := stream.NewStreamSubscriber(
		table,
		dynamodb.NewFromConfig(cfg),
		dynamodbstreams.NewFromConfig(streamCfg),
	)
	recordCh, errCh := subscriber.GetStreamData()

	go func() {
		for record := range recordCh {
			fmt.Println("from record channel: ", record)
		}
	}()

	go func() {
		for err := range errCh {
			fmt.Println("from error channel: ", err)
		}
	}()

	time.Sleep(100 * time.Second)
	
	subscriber.Shutdown()
}
```

## Deployment

If using this in actual deployment. There is a throttle on the shard implementation on AWS. That means that if you have a large deployment and have multiple calls towards the same shard AWS may very well throttle the calls resulting in ProvisionedThroughputExceededException and triggering a back-off.

The solution (before actually doing this) may be to have a 1:1 connection of applicatoin and shard to guarantee not hitting the limit.

