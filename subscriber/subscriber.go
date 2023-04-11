package subscriber

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dvictor/watermill-kinesis/closer"
)

// MessageHandler takes a message from Producer
// when the returned value `exit` is true, the producer needs to stop producing and exit current loop
type MessageHandler func(msg *message.Message) (exit bool)

// ProducerFactory creates Producer instances for the desired topic
// the handler func is stored in the Producer, to be called when a message is produced
type ProducerFactory func(handler MessageHandler, topic string) Producer

type Producer interface {
	// Start is a non-blocking call. It starts internal goroutine/s that produce messages.
	Start() error
	// Shutdown stops the producer and cleans up its resources. Should wait for all internal goroutines to exit.
	Shutdown()
}

func NewSubscriber(producerFactory ProducerFactory) *Subscriber {
	return &Subscriber{
		closer:  closer.NewCloser(),
		factory: producerFactory,
	}
}

type Subscriber struct {
	closer  closer.Closer
	factory ProducerFactory
	log     watermill.LoggerAdapter
}

var _ message.Subscriber = (*Subscriber)(nil)

func (s *Subscriber) Close() error {
	return s.closer.Close()
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	output := make(chan *message.Message)
	ctx = s.closer.Start(ctx)
	// we might be closed already, skip the start and return a closed chan
	select {
	case <-ctx.Done():
		close(output)
		return output, nil
	default:
	}

	producer := s.factory(func(msg *message.Message) bool {
		return s.processMessage(ctx, msg, output)
	}, topic)

	err := producer.Start()
	if err != nil {
		close(output)
		return output, err
	}

	go func() {
		<-ctx.Done()
		producer.Shutdown()
		close(output)
		s.closer.Done()
	}()

	return output, nil
}

func (s *Subscriber) processMessage(ctx context.Context, msg *message.Message, output chan *message.Message) bool {
	msg.SetContext(ctx)
	for {
		select {
		case output <- msg:
		case <-ctx.Done():
			return true
		}

		select {
		case <-ctx.Done():
			return true
		case <-msg.Acked():
			return false
		case <-msg.Nacked():
		}
		msg = msg.Copy()
		msg.SetContext(ctx)
	}
}
