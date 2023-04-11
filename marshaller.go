package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const PartitionKeyKey = "partitionKey"
const ShardIDKey = "shardID"
const ApproximateArrivalTimestampKey = "approximateArrivalTimestamp"

type MessageData struct {
	WatermillMessageUUID string            `json:"watermill_message_uuid"`
	Data                 string            `json:"data"`
	Headers              map[string]string `json:"headers"`
}

type Unmarshaller = func(record types.Record) (*message.Message, error)

// TODO Protobuf un/marshaller

// JSONUnmarshaller uses JSON to decode the message from Kinesis.
// Works with JSONMarshaller on the publisher side
func JSONUnmarshaller(record types.Record) (*message.Message, error) {
	var data MessageData
	err := json.Unmarshal(record.Data, &data)
	if err != nil {
		return nil, err
	}

	metadata := make(message.Metadata, 2)
	for key, value := range data.Headers {
		metadata[key] = value
	}
	metadata[PartitionKeyKey] = *record.PartitionKey
	metadata[ApproximateArrivalTimestampKey] = record.ApproximateArrivalTimestamp.Format(time.RFC3339)

	payload, err := base64.StdEncoding.DecodeString(data.Data)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(data.WatermillMessageUUID, payload)
	msg.Metadata = metadata
	return msg, nil
}

type Marshaller func(message *message.Message) (types.PutRecordsRequestEntry, error)

func JSONMarshaller(message *message.Message) (types.PutRecordsRequestEntry, error) {
	data := &MessageData{
		WatermillMessageUUID: message.UUID,
		Data:                 base64.StdEncoding.EncodeToString(message.Payload),
		Headers:              message.Metadata,
	}

	b, err := json.Marshal(data)
	if err != nil {
		return types.PutRecordsRequestEntry{}, err
	}
	partitionKey := message.Metadata[PartitionKeyKey]

	return types.PutRecordsRequestEntry{
		Data:         b,
		PartitionKey: &partitionKey,
	}, nil
}
