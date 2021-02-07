# Kafka Commands

## Starting zookeeper
	zookeeper-server-start config\zookeeper.properties

## Starting Kafka
	kafka-server-start config\server.properties

## Creating kafka topic
	kafka-topics --zookeeper 127.0.0.1:2181 --topic first_topic --create --partitions 3 --replication-factor 1

## List Topics
	kafka-topics --zookeeper 127.0.0.1:2181 --list

## Describe Topics
	kafka-topics --zookeeper 127.0.0.1:2181 --topic first_topic --describe

## Kafka Producer Console
	kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic

	Producer with keys

	kafka-console-producer --broker-list 127.0.0.1:9092 --topic first_topic --property parse.key=true --property key.separator=,
	> key,value
	> another key,another value
	
## Kafka Consumer Console
##### This will read only the streaming messages and not the ones already present in queue
	kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic

#####  This will read messages from beginning
	kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning

#####  Reading messages from offset from a particular partitions (--offset will not work without partition)
	kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --partition 1 --offset 0

##### Consumer group
	kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-second-application --from-beginning
	
##### Using Key Value
    kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --from-beginning --property print.key=true --property key.separator=,


## Kafka Cosnumer Groups
##### List all consumers
	kafka-consumer-groups --bootstrap-server localhost:9092 --list
	
##### Describe consumer group gives details of particular consumer group along with current and log-end offset
	kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-second-application

##### Resetting offsets in a consumer group
	1. Resetting to-earliest
		kafka-consumer-groups --bootstrap-server localhost:9092 --group my-second-application --reset-offsets --to-earliest --execute --topic first_topic
	2. Resetting shift-by
		kafka-consumer-groups --bootstrap-server localhost:9092 --group my-second-application --reset-offsets -shift-by -2 --execute --topic first_topic