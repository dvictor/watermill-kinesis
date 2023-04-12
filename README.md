# A Kinesis Pub/Sub for Watermill

This is a Pub/Sub backend for the popular Go library [Watermill](https://github.com/ThreeDotsLabs/watermill)

> Watermill is a Go library for working efficiently with message streams.
> It is intended for building event driven applications, enabling event
> sourcing, RPC over messages, sagas and basically whatever else comes to
> your mind. You can use conventional pub/sub implementations like Kafka or
> RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

Use with caution, this is alpha quality code for now.
Feedback and contributions are greatly appreciated.

Here's how to build a Subscriber:

```go
import "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
import "github.com/dvictor/watermill-kinesis"
import "github.com/ThreeDotsLabs/watermill"


  kclConfig := config.NewKinesisClientLibConfig(
    "samplestream",
    "unused", // topic will be overwritten in Subscribe
    "us-west-2",
    workerID, // workerID identifies this worker process
              // if the process is restarted, it would continue
              // consuming the same shards
  )
  // you can start many workers, depending on the number of shards
  // that your stream has
  // a base number can be 10 shards per worker
  
  subscriber, err := kinesis.SubscriberBuilder(kclConfig).Build()
  // this is where the topic (Kinesis stream name) is provided
  messages, err := subscriber.Subscribe(ctx, topic) 

  // for each worker, you can start processing messages in parallel
  // in multiple goroutines. tune this number for your workload
  for i := 0; i < 10; i++ {
     go func() {
      for msg := range messages {
       var data map[string]any
    
       logger.Info("received message", watermill.LogFields{
        "messageID": msg.UUID,
        "pkey":      msg.Metadata[kinesis.PartitionKeyKey],
        "shard":     msg.Metadata[kinesis.ShardIDKey],
        "payload":   string(msg.Payload),
       })
       // Must acknowledge that message was processed to avoid deadlock
       // if processing failed, can instead do msg.Nack() and the message will be served again on the channel
       // there's no limit to the number of retries, but one can use msg.Metadata["retryCount"] to store retries
       msg.Ack()
      }
     }()
  }
  // Note that you should use a WaitGroup to make sure all consumer
  // goroutines above finish their work for a clean exit.

  // Wait for a signal to stop the application.
  c := make(chan os.Signal, 1)
  signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
  <-c

```

For more detailed examples, check the example package.
A Localstack Kinesis in Docker Compose example is also provided.

Also, check this [introduction article](https://medium.com/@victor.dramba/kinesis-adapter-for-watermill-ed40eb4d396a)

