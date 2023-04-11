package main

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dvictor/watermill-kinesis"
)

func main() {
	const topic = "victor-vmware-kcl"

	opts := kinesis.ClientOptions{
		Region: "us-west-2",
	}
	// if using Localstack
	kinesis.WithEndpoint("http://127.0.0.1:4566")(&opts)
	publisher := kinesis.NewPublisher(opts)

	var messages []*message.Message

	for i := 0; i < 100; i++ {
		msg := message.NewMessage(watermill.NewUUID(), []byte(fmt.Sprintf("hello world! %d", i)))
		msg.Metadata[kinesis.PartitionKeyKey] = fmt.Sprintf("pkey%d", i)
		messages = append(messages, msg)
	}

	err := publisher.Publish(topic, messages...)
	if err != nil {
		panic(err)
	}
}
