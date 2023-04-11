package kinesis

import (
	"sync"

	"github.com/dvictor/watermill-kinesis/subscriber"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
)

type recordProcessorFactory struct {
	mu           sync.Mutex
	nextID       int
	handler      subscriber.MessageHandler
	log          watermill.LoggerAdapter
	unmarshaller Unmarshaller
}

func (r *recordProcessorFactory) CreateProcessor() interfaces.IRecordProcessor {
	r.mu.Lock()
	r.nextID++
	r.mu.Unlock()
	return &recordProcessor{
		id:           r.nextID,
		handler:      r.handler,
		log:          r.log,
		unmarshaller: r.unmarshaller,
	}
}

type recordProcessor struct {
	id           int
	shardID      string
	seq          *interfaces.ExtendedSequenceNumber
	handler      subscriber.MessageHandler
	log          watermill.LoggerAdapter
	unmarshaller Unmarshaller
}

func (r *recordProcessor) Initialize(input *interfaces.InitializationInput) {
	r.shardID = input.ShardId
	// TODO see if you can skip sent messages in first batch using this
	r.seq = input.ExtendedSequenceNumber
}

func (r *recordProcessor) ProcessRecords(input *interfaces.ProcessRecordsInput) {
	if len(input.Records) == 0 {
		return
	}
	for _, record := range input.Records {
		msg, err := r.unmarshaller(record)
		if err != nil {
			// if the message is not parsable, don't stop the app, skip message and log error
			r.log.Error("parsing record from Kinesis", err, nil)
			continue
		}
		msg.Metadata[ShardIDKey] = r.shardID
		if r.handler(msg) {
			return // early exit for shutdown, don't checkpoint, we will repeat messages in this batch
		}
	}
	seq := input.Records[len(input.Records)-1].SequenceNumber
	r.log.Debug("writing checkpoint", watermill.LogFields{
		"seq":   seq,
		"shard": r.shardID,
	})
	err := input.Checkpointer.Checkpoint(seq)
	if err != nil {
		r.log.Error("saving checkpoint", err, watermill.LogFields{
			"sequenceNumber": seq,
		})
	}
}

func (r *recordProcessor) Shutdown(input *interfaces.ShutdownInput) {
	if input.ShutdownReason == interfaces.TERMINATE {
		r.log.Debug("writing checkpoint at SHARD_END", watermill.LogFields{
			"shard": r.shardID,
		})
		err := input.Checkpointer.Checkpoint(nil)
		if err != nil {
			r.log.Error("saving SHARD_END checkpoint in Shutdown", err, nil)
		}
	}
}

type SubBuilder struct {
	kclConfig    *config.KinesisClientLibConfiguration
	unmarshaller Unmarshaller
	log          watermill.LoggerAdapter
}

func SubscriberBuilder(kclConfig *config.KinesisClientLibConfiguration) SubBuilder {
	return SubBuilder{
		kclConfig:    kclConfig,
		unmarshaller: JSONUnmarshaller,
		log:          NewStdLogger(true, false, false),
	}
}

func (b SubBuilder) WithLogger(logger watermill.LoggerAdapter) SubBuilder {
	b.log = logger
	return b
}

func (b SubBuilder) WithUnmarshaller(unmarshaller Unmarshaller) SubBuilder {
	b.unmarshaller = unmarshaller
	return b
}

func (b SubBuilder) Build() (*subscriber.Subscriber, error) {

	var factory subscriber.ProducerFactory = func(handler subscriber.MessageHandler, topic string) subscriber.Producer {
		b.kclConfig.StreamName = topic
		b.kclConfig.Logger = convertLogger(b.log)

		return worker.NewWorker(&recordProcessorFactory{
			log:          b.log,
			handler:      handler,
			unmarshaller: b.unmarshaller,
		}, b.kclConfig)
	}

	return subscriber.NewSubscriber(factory), nil
}
