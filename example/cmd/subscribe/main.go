package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/dvictor/watermill-kinesis"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
)

func main() {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		panic("no WORKER_ID in env")
	}

	topic := "victor-vmware-kcl"

	// Load the AWS SDK configuration.
	kclConfig := config.NewKinesisClientLibConfig(
		"victor-vmware-kcl",
		topic, // topic will be overwritten in Subscribe
		"us-west-2",
		workerID,
	)
	kclConfig.KinesisEndpoint = "http://127.0.0.1:4566"
	kclConfig.DynamoDBEndpoint = kclConfig.KinesisEndpoint

	kclConfig.MaxRecords = 100
	kclConfig.MaxLeasesForWorker = 6

	ctx := context.TODO()

	logger := kinesis.NewStdLogger(true, true, false)

	subscriber, err := kinesis.SubscriberBuilder(kclConfig).
		WithLogger(logger.WithDebug(false).WithPrefix("[watermill/kinesis] ")).
		Build()

	if err != nil {
		panic(err)
	}

	//metricsBuilder := metrics.NewPrometheusMetricsBuilder(promReg, "", "")
	//subscriber, err = metricsBuilder.DecorateSubscriber(subscriber)
	//if err != nil {
	//	panic(err)
	//}

	messages, err := subscriber.Subscribe(ctx, topic)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for msg := range messages {

			var data map[string]any
			_ = json.Unmarshal(msg.Payload, &data)

			logger.Info("received message", watermill.LogFields{
				"messageID": msg.UUID,
				"pkey":      msg.Metadata[kinesis.PartitionKeyKey],
				"shard":     msg.Metadata[kinesis.ShardIDKey],
				"payload":   fmt.Sprintf("%v", data),
			})
			// Must acknowledge that message was processed to avoid deadlock
			// if processing failed, can instead do msg.Nack() and the message will be served again on the channel
			// there's no limit to the number of retries, but one can use msg.Metadata["retryCount"] to store retries
			msg.Ack()
		}
		wg.Done()
	}()

	// Wait for a signal to stop the application.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	// Shutdown the subscriber.
	err = subscriber.Close()
	if err != nil {
		panic(err)
	}

	wg.Wait()
}
