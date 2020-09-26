# Tests scenarios

## Stateful

### `DropDuplicatesDemo`

#### 1.Setup
1.1. Start the broker:
```
cd broker/
docker-compose down --volumes; docker-compose up
```
1.2. Access the broker, create the topic and producer:
```
docker exec -ti broker_kafka_1 bin/bash
kafka-topics.sh --bootstrap-server localhost:29092 --topic drop_duplicates_topic --delete
kafka-topics.sh --bootstrap-server localhost:29092 --topic drop_duplicates_topic --create --partitions 2
kafka-console-producer.sh --bootstrap-server localhost:29092 --topic drop_duplicates_topic
```
1.3. Start `DropDuplicatesDemo` in debug mode

1.4. Go back to **1.2** and produce the following records:
```
{"event_time": "2020-05-05T01:22:00", "id": 1, "value": 1}
TODO: to terminate
```

### `MultipleStateOperationsDemo`

#### 1.Setup
1.1. Start the broker:
```
cd broker/
docker-compose down --volumes; docker-compose up
```
1.2. Access the broker, create the topic and producer:
```
docker exec -ti broker_kafka_1 bin/bash
kafka-topics.sh --bootstrap-server localhost:29092 --topic multiple_states_topic --delete
kafka-topics.sh --bootstrap-server localhost:29092 --topic multiple_states_topic --create --partitions 2
kafka-console-producer.sh --bootstrap-server localhost:29092 --topic multiple_states_topic
```
1.3. Start `DropDuplicatesDemo` in debug mode

1.4. Go back to **1.2** and produce the following records:
```
{"event_time": "2020-05-05T01:22:00", "id": 1, "value": 1}
TODO: to terminate
```


### `GlobalLimitDemo`

#### 1.Setup
1.1. Start the broker:
```
cd broker/
docker-compose down --volumes; docker-compose up
```
1.2. Access the broker, create the topic and producer:
```
docker exec -ti broker_kafka_1 bin/bash
kafka-topics.sh --bootstrap-server localhost:29092 --topic global_limit_topic --delete
kafka-topics.sh --bootstrap-server localhost:29092 --topic global_limit_topic --create --partitions 2
kafka-console-producer.sh --bootstrap-server localhost:29092 --topic global_limit_topic
```
1.3. Start `DropDuplicatesDemo` in debug mode

1.4. Go back to **1.2** and produce the following records:
```
{"event_time": "2020-05-05T01:22:00", "id": 1, "value": 1}
TODO: to terminate
```