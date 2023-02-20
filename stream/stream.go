// Yury Kozyrev (urakozz)
// MIT License
package stream

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"sync"
	"time"
)

type StreamSubscriber struct {
	dynamoSvc            DynamoService
	streamSvc            StreamService
	table                *string
	ShardIteratorType    types.ShardIteratorType
	ShardProcessingLimit *int
	Limit                *int32
	KeepOrder            *bool
	shutdownCh           chan struct{}
	closeChannelOnce     sync.Once
}

func NewStreamSubscriber(
	dynamoSvc DynamoService,
	streamSvc StreamService,
	table string) *StreamSubscriber {
	s := &StreamSubscriber{
		shutdownCh: make(chan struct{}),
		dynamoSvc:  dynamoSvc,
		streamSvc:  streamSvc,
		table:      &table,
	}
	s.applyDefaults()
	return s
}

func (r *StreamSubscriber) applyDefaults() {
	if len(r.ShardIteratorType) == 0 {
		r.ShardIteratorType = types.ShardIteratorTypeLatest
	}
	if r.KeepOrder == nil {
		r.SetKeepOrder(false)
	}
	if r.ShardProcessingLimit == nil || *r.ShardProcessingLimit == 0 {
		r.ShardProcessingLimit = aws.Int(5)
	}

}

func (r *StreamSubscriber) SetLimit(v int32) {
	r.Limit = aws.Int32(v)
}

func (r *StreamSubscriber) SetShardIteratorType(shardIteratorType types.ShardIteratorType) {
	r.ShardIteratorType = shardIteratorType
}

func (r *StreamSubscriber) SetShardProcessingLimit(shardProcessingLimit int) {
	r.ShardProcessingLimit = aws.Int(shardProcessingLimit)
}

func (r *StreamSubscriber) SetKeepOrder(b bool) {
	r.KeepOrder = aws.Bool(b)
}

func (r *StreamSubscriber) Shutdown() {
	close(r.shutdownCh)
}

func (r *StreamSubscriber) GetStreamData() (<-chan *types.Record, <-chan error) {
	ch := make(chan *types.Record, 1)
	errCh := make(chan error, 1)

	go func(ch chan<- *types.Record, errCh chan<- error) {
		var shardId *string
		var prevShardId *string
		var streamArn *string
		var err error

		for {
			prevShardId = shardId
			shardId, streamArn, err = r.findProperShardId(prevShardId)
			if err != nil {
				errCh <- err
			}
			if shardId != nil {
				err = r.processShardBackport(shardId, streamArn, ch)
				if err != nil {
					errCh <- err
					// reset shard id to process it again
					shardId = prevShardId
				}
			}
			select {
			case _ = <-r.shutdownCh:
				return
			default:
				if shardId == nil {
					time.Sleep(time.Second * 10)
				}
			}
		}
	}(ch, errCh)

	return ch, errCh
}

func (r *StreamSubscriber) GetStreamDataAsync() (<-chan *types.Record, <-chan error) {
	ch := make(chan *types.Record, 1)
	errCh := make(chan error, 1)

	needUpdateChannel := make(chan struct{}, 1)
	needUpdateChannel <- struct{}{}

	allShards := new(sync.Map)
	shardsCh := make(chan *dynamodbstreams.GetShardIteratorInput, *r.ShardProcessingLimit)

	go func() {
		tick := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-r.shutdownCh:
				return
			case <-tick.C:
				needUpdateChannel <- struct{}{}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-r.shutdownCh:
				return
			case <-needUpdateChannel:
				streamArn, err := r.getLatestStreamArn()
				if err != nil {
					errCh <- err
					return
				}
				ids, err := r.getShardIds(streamArn)
				if err != nil {
					errCh <- err
					return
				}
				for _, sObj := range ids {
					// Consume the child shard after its parent shard is completed
					// Parent shard must be processed before its children to ensure that
					// the stream records are also processed in the correct order.
					// More Details https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
					if sObj.ParentShardId != nil {
						// FIXME: Although "parentExists" is false, Parent shard might be at the end of shard ids array. So it doesn't work with "KeepOrder".
						if parentCompleted, parentExist := allShards.Load(*sObj.ParentShardId); !(*r.KeepOrder && parentExist && !isTrue(parentCompleted)) {
							if _, ok := allShards.LoadOrStore(*sObj.ShardId, struct{}{}); !ok {
								shardsCh <- &dynamodbstreams.GetShardIteratorInput{
									StreamArn:         streamArn,
									ShardId:           sObj.ShardId,
									ShardIteratorType: r.ShardIteratorType,
								}
							}
						}
					} else {
						// This may not be the correct solution here. But rather go through all shards and mark them as
						// not processed then afterwards see whether we processed any shards. If not we only have dangling
						// children and go back and process them. However, I think this only occurs if we only have one
						// shard.
						if _, ok := allShards.LoadOrStore(*sObj.ShardId, struct{}{}); !ok {
							shardsCh <- &dynamodbstreams.GetShardIteratorInput{
								StreamArn:         streamArn,
								ShardId:           sObj.ShardId,
								ShardIteratorType: r.ShardIteratorType,
							}
						}
					}
				}
			}
		}
	}()

	limit := make(chan struct{}, *r.ShardProcessingLimit)
	go func() {
		for {
			select {
			case _ = <-r.shutdownCh:
				return
			case shardInput := <-shardsCh:
				limit <- struct{}{}
				go func(sInput *dynamodbstreams.GetShardIteratorInput) {
					if err := r.processShard(sInput, ch); err != nil {
						errCh <- err
					}
					// Mark the shard is completed
					allShards.Store(*sInput.ShardId, true)
					<-limit
					select {
					case _ = <-r.shutdownCh:
						if len(limit) == 0 {
							r.closeChannelOnce.Do(func() {
								close(ch)
								close(errCh)
							})
						}
					default:
					}
				}(shardInput)
			}
		}
	}()

	return ch, errCh
}

func (r *StreamSubscriber) getShardIds(streamArn *string) (ids []types.Shard, err error) {
	des, err := r.streamSvc.DescribeStream(context.Background(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	})
	if err != nil {
		return nil, err
	}
	// No shards
	if 0 == len(des.StreamDescription.Shards) {
		return nil, nil
	}

	return des.StreamDescription.Shards, nil
}

func (r *StreamSubscriber) findProperShardId(previousShardId *string) (shadrId *string, streamArn *string, err error) {
	streamArn, err = r.getLatestStreamArn()
	if err != nil {
		return nil, nil, err
	}
	des, err := r.streamSvc.DescribeStream(context.Background(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	})
	if err != nil {
		return nil, nil, err
	}

	if 0 == len(des.StreamDescription.Shards) {
		return nil, nil, nil
	}

	if previousShardId == nil {
		shadrId = des.StreamDescription.Shards[0].ShardId
		return
	}

	for _, shard := range des.StreamDescription.Shards {
		shadrId = shard.ShardId
		if shard.ParentShardId != nil && *shard.ParentShardId == *previousShardId {
			return
		}
	}

	return
}

func (r *StreamSubscriber) getLatestStreamArn() (*string, error) {
	tableInfo, err := r.dynamoSvc.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: r.table})
	if err != nil {
		return nil, err
	}
	if nil == tableInfo.Table.LatestStreamArn {
		return nil, errors.New("empty table stream arn")
	}
	return tableInfo.Table.LatestStreamArn, nil
}

func (r *StreamSubscriber) processShardBackport(shardId, lastStreamArn *string, ch chan<- *types.Record) error {
	return r.processShard(&dynamodbstreams.GetShardIteratorInput{
		StreamArn:         lastStreamArn,
		ShardId:           shardId,
		ShardIteratorType: r.ShardIteratorType,
	}, ch)
}

func (r *StreamSubscriber) processShard(input *dynamodbstreams.GetShardIteratorInput, ch chan<- *types.Record) error {
	iter, err := r.streamSvc.GetShardIterator(context.Background(), input)
	if err != nil {
		return err
	}
	if iter.ShardIterator == nil {
		return nil
	}

	nextIterator := iter.ShardIterator
	for nextIterator != nil {
		recs, err := r.streamSvc.GetRecords(context.Background(), &dynamodbstreams.GetRecordsInput{
			ShardIterator: nextIterator,
			Limit:         r.Limit,
		})
		if err != nil {
			if _, ok := err.(*types.TrimmedDataAccessException); ok {
				// Trying to request data older than 24h, that's ok
				// http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetShardIterator.html -> Errors
				return nil
			}
			return err
		}

		for _, record := range recs.Records {
			ch <- &record
		}

		nextIterator = recs.NextShardIterator

		sleepDuration := time.Second
		// Nil next itarator, shard is closed
		if nextIterator == nil {
			sleepDuration = time.Millisecond * 10
		} else if len(recs.Records) == 0 {
			// Empty set, but shard is not closed -> sleep a little
			sleepDuration = time.Second * 10
		}

		after := time.After(sleepDuration)
		select {
		case _ = <-r.shutdownCh:
			return nil
		case _ = <-after:
		}
	}

	return nil
}
