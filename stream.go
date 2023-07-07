package stream

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"runtime"
	"sync"
	"time"
)

type StreamSubscriber struct {
	table string

	dynamoSvc DynamoService
	streamSvc StreamService

	processingShards sync.Map

	shardProcessQueue chan *shardProcessContext

	shutdownCh chan struct{}
	recordCh   chan *types.Record
	errorCh    chan error

	shardIteratorType            types.ShardIteratorType
	shardIteratorInitialInterval time.Duration
	shardIteratorMaxInterval     time.Duration
	shardUpdateInterval          time.Duration
	shardProcessWorkers          int
	maximumRecords               int32
}

func NewStreamSubscriber(
	table string,
	dynamoSvc DynamoService,
	streamSvc StreamService,
) *StreamSubscriber {
	s := &StreamSubscriber{
		table:            table,
		dynamoSvc:        dynamoSvc,
		streamSvc:        streamSvc,
		processingShards: sync.Map{},
		shutdownCh:       make(chan struct{}),
	}
	s.applyDefaults()
	return s
}

func (r *StreamSubscriber) applyDefaults() {
	r.SetShardIteratorType(types.ShardIteratorTypeLatest)
	r.SetShardIteratorInitialInterval(100 * time.Millisecond)
	r.SetShardIteratorMaxInterval(1 * time.Minute)
	r.SetShardUpdateInterval(1 * time.Minute)
	r.SetShardProcessWorkers(runtime.NumCPU())
	r.SetShardProcessQueueSize(8192)
	r.SetMaximumRecords(1000)
	r.SetRecordBufferSize(8192)
	r.SetErrorBufferSize(8192)
}

func (r *StreamSubscriber) SetShardIteratorType(shardIteratorType types.ShardIteratorType) {
	if len(r.shardIteratorType) == 0 {
		r.shardIteratorType = shardIteratorType
	}
}

func (r *StreamSubscriber) SetShardIteratorInitialInterval(shardIteratorInitialInterval time.Duration) {
	if r.shardIteratorInitialInterval == 0 {
		r.shardIteratorInitialInterval = shardIteratorInitialInterval
	}
}

func (r *StreamSubscriber) SetShardIteratorMaxInterval(shardIteratorMaxInterval time.Duration) {
	if r.shardIteratorMaxInterval == 0 {
		r.shardIteratorMaxInterval = shardIteratorMaxInterval
	}
}

func (r *StreamSubscriber) SetShardUpdateInterval(shardUpdateInterval time.Duration) {
	if r.shardUpdateInterval == 0 {
		r.shardUpdateInterval = shardUpdateInterval
	}
}

func (r *StreamSubscriber) SetShardProcessWorkers(shardProcessWorkers int) {
	if r.shardProcessWorkers == 0 {
		r.shardProcessWorkers = shardProcessWorkers
	}
}

func (r *StreamSubscriber) SetShardProcessQueueSize(shardProcessQueueSize int) {
	if r.shardProcessQueue == nil {
		r.shardProcessQueue = make(chan *shardProcessContext, shardProcessQueueSize)
	}
}

func (r *StreamSubscriber) SetMaximumRecords(maximumRecords int32) {
	if r.maximumRecords == 0 {
		r.maximumRecords = maximumRecords
	}
}

func (r *StreamSubscriber) SetRecordBufferSize(bufferSize int32) {
	if r.recordCh == nil {
		r.recordCh = make(chan *types.Record, bufferSize)
	}
}

func (r *StreamSubscriber) SetErrorBufferSize(bufferSize int32) {
	if r.errorCh == nil {
		r.errorCh = make(chan error, bufferSize)
	}
}

func (r *StreamSubscriber) Shutdown() {
	defer func() {
		recover()
	}()
	close(r.shutdownCh)
	close(r.errorCh)
	close(r.recordCh)
}

func (r *StreamSubscriber) GetStreamData() (<-chan *types.Record, <-chan error) {
	go func() {
		shardUpdateInterval := time.Duration(0)
		for {
			select {
			case <-r.shutdownCh:
				return
			case <-time.After(shardUpdateInterval):
				if shardUpdateInterval == 0 {
					shardUpdateInterval = r.shardUpdateInterval
				}
				streamArn, err := r.getLatestStreamArn()
				if err != nil {
					r.sendError(err)
					var resourceNotFoundException *dynamodbTypes.ResourceNotFoundException
					if errors.As(err, &resourceNotFoundException) {
						r.Shutdown()
						return
					}
					continue
				}
				if streamArn == nil {
					r.sendError(errors.New("stream arn is nil"))
					r.Shutdown()
					return
				}
				shards, err := r.getShards(streamArn)
				if err != nil {
					r.sendError(err)
					var resourceNotFoundException *dynamodbTypes.ResourceNotFoundException
					if errors.As(err, &resourceNotFoundException) {
						r.Shutdown()
						return
					}
					continue
				}
				for _, shard := range shards {
					if _, exist := r.processingShards.LoadOrStore(*shard.ShardId, struct{}{}); !exist {
						r.shardProcessQueue <- newShardProcessContext(
							&dynamodbstreams.GetShardIteratorInput{
								StreamArn:         streamArn,
								ShardId:           shard.ShardId,
								ShardIteratorType: r.shardIteratorType,
							},
							r.shardIteratorInitialInterval,
							r.shardIteratorMaxInterval,
						)
					}
				}
			}
		}
	}()

	go func() {
		shardProcessWorkersCh := make(chan struct{}, r.shardProcessWorkers)
		for {
			select {
			case <-r.shutdownCh:
				return
			case shardProcessCtx := <-r.shardProcessQueue:
				shardProcessWorkersCh <- struct{}{}
				go func(processCtx *shardProcessContext) {
					if err := r.processShard(processCtx); err != nil {
						r.sendError(err)
					}
					if !processCtx.done {
						go func() {
							select {
							case <-r.shutdownCh:
							case <-time.After(processCtx.backoff.NextBackOff()):
								r.shardProcessQueue <- processCtx
							}
						}()
					}
					<-shardProcessWorkersCh
				}(shardProcessCtx)
			}
		}
	}()

	return r.recordCh, r.errorCh
}

func (r *StreamSubscriber) sendRecord(record *types.Record) {
	select {
	case <-r.shutdownCh:
	case r.recordCh <- record:
	}
}

func (r *StreamSubscriber) sendError(err error) {
	select {
	case <-r.shutdownCh:
	case r.errorCh <- err:
	}
}

func (r *StreamSubscriber) getLatestStreamArn() (*string, error) {
	tableOutput, err := r.dynamoSvc.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: &r.table,
	})
	if err != nil {
		return nil, err
	}
	return tableOutput.Table.LatestStreamArn, nil
}

func (r *StreamSubscriber) getShards(streamArn *string) (shards []types.Shard, err error) {
	des, err := r.streamSvc.DescribeStream(context.Background(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	})
	if err != nil {
		return nil, err
	}
	return des.StreamDescription.Shards, nil
}

func (r *StreamSubscriber) processShard(processCtx *shardProcessContext) error {
	if processCtx.iterator == nil {
		iterator, err := r.streamSvc.GetShardIterator(context.Background(), processCtx.input)
		if err != nil {
			return err
		}

		if iterator.ShardIterator == nil {
			processCtx.done = true
			return nil
		}

		processCtx.iterator = iterator.ShardIterator
	}

	output, err := r.streamSvc.GetRecords(context.Background(), &dynamodbstreams.GetRecordsInput{
		ShardIterator: processCtx.iterator,
		Limit:         aws.Int32(r.maximumRecords),
	})
	if err != nil {
		var expiredIteratorException *types.ExpiredIteratorException
		if errors.As(err, &expiredIteratorException) {
			processCtx.iterator = nil
			return nil
		}

		var trimmedDataAccessException *types.TrimmedDataAccessException
		if errors.As(err, &trimmedDataAccessException) {
			processCtx.done = true
			return nil
		}

		var resourceNotFoundException *types.ResourceNotFoundException
		if errors.As(err, &resourceNotFoundException) {
			processCtx.done = true
			return nil
		}
		return err
	}

	for _, record := range output.Records {
		r.sendRecord(&record)
	}

	if output.NextShardIterator == nil {
		processCtx.done = true
		return nil
	}
	processCtx.iterator = output.NextShardIterator

	if len(output.Records) > 0 {
		processCtx.backoff.Reset()
	}

	return nil
}

type shardProcessContext struct {
	input    *dynamodbstreams.GetShardIteratorInput
	iterator *string
	done     bool
	backoff  *backoff.ExponentialBackOff
}

func newShardProcessContext(
	input *dynamodbstreams.GetShardIteratorInput,
	initialInterval time.Duration,
	maxInterval time.Duration,
) *shardProcessContext {
	bo := backoff.NewExponentialBackOff()
	bo.Multiplier = 2
	bo.RandomizationFactor = 0.1
	bo.InitialInterval = initialInterval
	bo.MaxInterval = maxInterval
	bo.Reset()
	return &shardProcessContext{
		input:   input,
		backoff: bo,
	}
}
