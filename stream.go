package stream

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/smithy-go"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Subscriber struct {
	table string

	dynamoSvc DynamoService
	streamSvc StreamService

	shards sync.Map

	shardProcessQueue chan *shardProcessContext

	shutdownCh chan struct{}
	recordCh   chan *types.Record
	errorCh    chan error

	shardSequenceIteratorType    types.ShardIteratorType
	shardIteratorType            types.ShardIteratorType
	shardIteratorInitialInterval time.Duration
	shardIteratorMaxInterval     time.Duration
	shardUpdateInterval          time.Duration
	shardProcessWorkers          int
	maximumRecords               int32
}

func NewSubscriber(
	dynamoSvc DynamoService,
	streamSvc StreamService,
	table string,
) *Subscriber {
	s := &Subscriber{
		table:      table,
		dynamoSvc:  dynamoSvc,
		streamSvc:  streamSvc,
		shards:     sync.Map{},
		shutdownCh: make(chan struct{}),
	}
	s.applyDefaults()
	return s
}

func (r *Subscriber) applyDefaults() {
	r.SetShardSequenceIteratorType(types.ShardIteratorTypeAfterSequenceNumber)
	r.SetShardIteratorType(types.ShardIteratorTypeTrimHorizon)
	r.SetShardIteratorInitialInterval(1 * time.Second)
	r.SetShardIteratorMaxInterval(1 * time.Minute)
	r.SetShardUpdateInterval(1 * time.Minute)
	r.SetShardProcessWorkers(runtime.NumCPU())
	r.SetShardProcessQueueSize(8192)
	r.SetMaximumRecords(1000)
	r.SetRecordBufferSize(8192)
	r.SetErrorBufferSize(8192)
}

func (r *Subscriber) SetShardSequences(shardSequences []*ShardSequence) {
	for _, shardSequence := range shardSequences {
		if len(shardSequence.SequenceNumber) > 0 {
			r.shards.Store(shardSequence.ShardId, shardSequence.SequenceNumber)
		}
	}
}

func (r *Subscriber) SetShardSequenceIteratorType(shardSequenceIteratorType types.ShardIteratorType) {
	if len(r.shardSequenceIteratorType) == 0 {
		switch shardSequenceIteratorType {
		case types.ShardIteratorTypeAtSequenceNumber:
		case types.ShardIteratorTypeAfterSequenceNumber:
		default:
			return
		}
		r.shardSequenceIteratorType = shardSequenceIteratorType
	}
}

func (r *Subscriber) SetShardIteratorType(shardIteratorType types.ShardIteratorType) {
	if len(r.shardIteratorType) == 0 {
		switch shardIteratorType {
		case types.ShardIteratorTypeTrimHorizon:
		case types.ShardIteratorTypeLatest:
		default:
			return
		}
		r.shardIteratorType = shardIteratorType
	}
}

func (r *Subscriber) SetShardIteratorInitialInterval(shardIteratorInitialInterval time.Duration) {
	if r.shardIteratorInitialInterval == 0 {
		r.shardIteratorInitialInterval = shardIteratorInitialInterval
	}
}

func (r *Subscriber) SetShardIteratorMaxInterval(shardIteratorMaxInterval time.Duration) {
	if r.shardIteratorMaxInterval == 0 {
		r.shardIteratorMaxInterval = shardIteratorMaxInterval
	}
}

func (r *Subscriber) SetShardUpdateInterval(shardUpdateInterval time.Duration) {
	if r.shardUpdateInterval == 0 {
		r.shardUpdateInterval = shardUpdateInterval
	}
}

func (r *Subscriber) SetShardProcessWorkers(shardProcessWorkers int) {
	if r.shardProcessWorkers == 0 {
		r.shardProcessWorkers = shardProcessWorkers
	}
}

func (r *Subscriber) SetShardProcessQueueSize(shardProcessQueueSize int) {
	if r.shardProcessQueue == nil {
		r.shardProcessQueue = make(chan *shardProcessContext, shardProcessQueueSize)
	}
}

func (r *Subscriber) SetMaximumRecords(maximumRecords int32) {
	if r.maximumRecords == 0 {
		r.maximumRecords = maximumRecords
	}
}

func (r *Subscriber) SetRecordBufferSize(bufferSize int32) {
	if r.recordCh == nil {
		r.recordCh = make(chan *types.Record, bufferSize)
	}
}

func (r *Subscriber) SetErrorBufferSize(bufferSize int32) {
	if r.errorCh == nil {
		r.errorCh = make(chan error, bufferSize)
	}
}

func (r *Subscriber) ShardSequences() []*ShardSequence {
	var shardSequences []*ShardSequence
	r.shards.Range(func(key any, value any) bool {
		shardId := key.(string)
		sequenceNumber := value.(string)
		if len(sequenceNumber) > 0 {
			shardSequences = append(shardSequences, &ShardSequence{
				ShardId:        shardId,
				SequenceNumber: sequenceNumber,
			})
		}
		return true
	})
	sort.Slice(shardSequences, func(i, j int) bool {
		return shardSequences[i].ShardId < shardSequences[j].ShardId
	})
	return shardSequences
}

func (r *Subscriber) Shutdown() {
	defer func() {
		recover()
	}()
	close(r.shutdownCh)
	close(r.errorCh)
}

func (r *Subscriber) Subscribe() (<-chan *types.Record, <-chan error) {
	go func() {
		for {
			select {
			case <-r.shutdownCh:
				time.Sleep(1 * time.Second)
				if len(r.recordCh) == 0 {
					close(r.recordCh)
					return
				}
			}
		}
	}()

	go func() {
		first := true
		after := time.Duration(0)
		for {
			select {
			case <-r.shutdownCh:
				return
			case <-time.After(after):
				after = r.shardUpdateInterval
				streamArn, err := r.getLatestStreamArn()
				if err != nil {
					r.sendError(err)
					continue
				}
				if streamArn == nil {
					r.sendError(errors.New("stream arn is nil"))
					continue
				}
				shards, err := r.getShards(streamArn)
				if err != nil {
					r.sendError(err)
					continue
				}
				if first {
					r.shards.Range(func(key any, value any) bool {
						shardId := key.(string)
						sequenceNumber := value.(string)
						r.shardProcessQueue <- newShardProcessContext(
							&dynamodbstreams.GetShardIteratorInput{
								StreamArn:         streamArn,
								ShardIteratorType: r.shardSequenceIteratorType,
								ShardId:           &shardId,
								SequenceNumber:    &sequenceNumber,
							},
							r.shardIteratorInitialInterval,
							r.shardIteratorMaxInterval,
						)
						return true
					})
					first = false
				}
				for _, shard := range shards {
					if _, exist := r.shards.LoadOrStore(*shard.ShardId, ""); !exist {
						r.shardProcessQueue <- newShardProcessContext(
							&dynamodbstreams.GetShardIteratorInput{
								StreamArn:         streamArn,
								ShardIteratorType: r.shardIteratorType,
								ShardId:           shard.ShardId,
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
					done, err := r.processShard(processCtx)
					if err != nil {
						r.sendError(err)
					}
					if !done {
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

func (r *Subscriber) sendRecord(record *types.Record) {
	select {
	case <-r.shutdownCh:
	case r.recordCh <- record:
	}
}

func (r *Subscriber) sendError(err error) {
	select {
	case <-r.shutdownCh:
	case r.errorCh <- err:
	}
}

func (r *Subscriber) getLatestStreamArn() (*string, error) {
	tableOutput, err := r.dynamoSvc.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{
		TableName: &r.table,
	})
	if err != nil {
		return nil, err
	}
	return tableOutput.Table.LatestStreamArn, nil
}

func (r *Subscriber) getShards(streamArn *string) (shards []types.Shard, err error) {
	des, err := r.streamSvc.DescribeStream(context.Background(), &dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	})
	if err != nil {
		return nil, err
	}
	return des.StreamDescription.Shards, nil
}

func (r *Subscriber) processShard(processCtx *shardProcessContext) (bool, error) {
	if processCtx.iterator == nil {
		iterator, err := r.streamSvc.GetShardIterator(context.Background(), processCtx.input)
		if err != nil {
			var apiErr *smithy.GenericAPIError
			if errors.As(err, &apiErr) && apiErr.Code == "ValidationException" {
				err = errors.Wrapf(err, "shard=%s, sequenceNumber=%s",
					*processCtx.input.ShardId,
					*processCtx.input.SequenceNumber)
				processCtx.input.SequenceNumber = nil
				processCtx.input.ShardIteratorType = r.shardIteratorType
			}
			return false, err
		}

		if iterator.ShardIterator == nil {
			return true, nil
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
			return false, nil
		}

		var trimmedDataAccessException *types.TrimmedDataAccessException
		if errors.As(err, &trimmedDataAccessException) {
			return true, nil
		}

		var resourceNotFoundException *types.ResourceNotFoundException
		if errors.As(err, &resourceNotFoundException) {
			return true, nil
		}

		return false, err
	}

	for _, record := range output.Records {
		r.sendRecord(&record)
	}

	if output.NextShardIterator == nil {
		return true, nil
	}
	processCtx.iterator = output.NextShardIterator

	if len(output.Records) > 0 {
		processCtx.backoff.Reset()
		r.shards.Store(*processCtx.input.ShardId, *output.Records[len(output.Records)-1].Dynamodb.SequenceNumber)
	}

	return false, nil
}

type shardProcessContext struct {
	input    *dynamodbstreams.GetShardIteratorInput
	iterator *string
	backoff  *backoff.ExponentialBackOff
}

func newShardProcessContext(
	input *dynamodbstreams.GetShardIteratorInput,
	initialInterval time.Duration,
	maxInterval time.Duration,
) *shardProcessContext {
	bo := backoff.NewExponentialBackOff()
	bo.RandomizationFactor = 0.1
	bo.InitialInterval = initialInterval
	bo.MaxInterval = maxInterval
	bo.Reset()
	return &shardProcessContext{
		input:   input,
		backoff: bo,
	}
}
