
STREAM_NAME=victor-vmware-kcl

#alias aws='aws --endpoint-url=http://localhost:4566'


# example write_batch_n 100 'hello world!'
#the data param is a JSON string
write_batch_n() {
  local cnt="$1"
  local data
  data="$(echo "$2" | base64)"

  recs=$(
    for i in $(seq 1 "$cnt"); do
      local uuid msg json
      uuid=$(uuidgen | tr -d '\n' | tr '[:upper:]' '[:lower:]')
      json="{\"watermill_message_uuid\":\"$uuid\", \"data\":$data}"
      msg=$(echo -n "$json" | base64)
      echo -n "Data=$msg,PartitionKey=pkey$i "
    done
  )

  aws kinesis put-records --debug --no-cli-pager --stream-name "$STREAM_NAME" --records $recs
}

createstream() {
  aws kinesis create-stream --stream-name "$STREAM_NAME" --shard-count "$1"
}

reshard() {
  aws kinesis update-shard-count --stream-name "$STREAM_NAME" --scaling-type UNIFORM_SCALING --target-shard-count "$1"
}