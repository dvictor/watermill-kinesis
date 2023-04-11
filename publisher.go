package kinesis

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const maxMessages = 500

type ClientOptions = kinesis.Options

type PubBuilder struct {
	options    *ClientOptions
	optFuncs   []func(options *ClientOptions)
	marshaller Marshaller
}

func PublisherBuilder(options *ClientOptions, optFuncs ...func(options *ClientOptions)) PubBuilder {
	return PubBuilder{
		options:    options,
		optFuncs:   optFuncs,
		marshaller: JSONMarshaller,
	}
}

type resolver func(region string, options kinesis.EndpointResolverOptions) (aws.Endpoint, error)

func (r resolver) ResolveEndpoint(region string, options kinesis.EndpointResolverOptions) (aws.Endpoint, error) {
	return r(region, options)
}

func (b PubBuilder) WithEndpoint(endpoint string) PubBuilder {
	b.options.EndpointResolver = resolver(func(region string, options kinesis.EndpointResolverOptions) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           endpoint,
			SigningRegion: region,
		}, nil
	})
	return b
}

func (b PubBuilder) WithMarshaller(marshaller Marshaller) PubBuilder {
	b.marshaller = marshaller
	return b
}

func (b PubBuilder) Build() *Publisher {
	return &Publisher{
		client: kinesis.New(*b.options, b.optFuncs...),
	}
}

type Publisher struct {
	client     *kinesis.Client
	marshaller Marshaller
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	// TODO split batches
	if len(messages) > maxMessages {
		return fmt.Errorf("max number of messages in one call exceeded. Received: %d, max: %d", len(messages), maxMessages)
	}

	var records []types.PutRecordsRequestEntry
	for _, msg := range messages {
		item, err := p.marshaller(msg)
		if err != nil {
			return err
		}
		records = append(records, item)
	}

	request := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: &topic,
		StreamARN:  aws.String(""),
	}
	_, err := p.client.PutRecords(context.Background(), request)
	return err
}

func (p *Publisher) Close() error {
	return nil
}
