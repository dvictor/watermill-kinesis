package subscriber_test

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dvictor/watermill-kinesis/subscriber"
	"sync"
	"testing"
	"time"
)

type testProducer struct {
	handler subscriber.MessageHandler
	close   chan struct{}
}

func (t *testProducer) Start() error {
	return nil
}

func (t *testProducer) Shutdown() {
	close(t.close)
}

func TestSubscriber(t *testing.T) {
	producer := &testProducer{close: make(chan struct{})}

	var factory subscriber.ProducerFactory = func(handler subscriber.MessageHandler, topic string) subscriber.Producer {
		producer.handler = handler
		return producer
	}

	wg1 := sync.WaitGroup{}

	s := subscriber.NewSubscriber(factory)

	// begin readers
	ctx := context.Background()

	var mu sync.Mutex
	var resultCount int

	var wg2 sync.WaitGroup

	subscribeCnt := 50
	ch, err := s.Subscribe(ctx, "")
	for i := 0; i < subscribeCnt; i++ {
		wg2.Add(1)
		if err != nil {
			panic(err)
		}
		i := i
		go func() {
			defer wg2.Done()
			n := 0
			for m := range ch {
				time.Sleep(time.Millisecond)
				m.Ack()
				n++
			}
			mu.Lock()
			resultCount += n
			t.Log("consumed in sub:", i, "count:", n)
			mu.Unlock()
		}()
	}
	// end readers

	loops := 10
	writers := 10
	rowSize := 10
	writtenCount := rowSize * loops * writers
	for g := 0; g < writers; g++ {
		wg1.Add(1)
		g := g
		go func() {
			for i := 0; i < loops; i++ {
				for j := 0; j < rowSize; j++ {
					msg := message.NewMessage(fmt.Sprint(g, i, j), nil)
					exit := producer.handler(msg)
					if exit {
						break
					}
				}

			}
			wg1.Done()
		}()
	}

	wg1.Wait()
	_ = s.Close()

	wg2.Wait()

	if resultCount != writtenCount {
		t.Fatalf("expecting %d messages, got: %d", writtenCount, resultCount)
	}
	t.Logf("total %d messages from %d writers, groups of %d", resultCount, writers, rowSize)
}
