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
		
		
# Important Configuration for Producer

## Producer Acks Deep Dive
### acks=0 (no acks) 	
    1. No response is requested.
    2. If broker goes offline or an exception happens, we won't know and will lose data
    3. Useful for data where it's okay to potentially lose messages:
        ex: Matrics collection, Log Collection
        
### acks = 1 (leader acks)  DEFAULT
    1. Leader response is requested, but there's no guarantee of replication. (Happens in a background)
    2. If an ack is not received, then the producer may retry.
    3. If the leader goes down before the replicas have the chance to replicate the data yet, then we'll have a data loss.
        
### acks = all (replicas acks)
    1. Leader + Replicas ack requested
    2. A bit of latency is added but more safety is there if you don't want to loose data
    3. Acks=all must be used in conjunctio with min.insync.replicas
    4. min.insync.replicas can be set at broker or topic level(override).
    5. min.insync.replicas=2 implies that at least 2 broker that are ISR (Including leader) must respond that they have the data
    6. That means that if you use replication.factor=3, min.insync.replica=2, and acks equals all, you can only tolerate one broker going down otherwise, the producer will receive an exception on send.

##Producer Retries
    In case of transient failures, the developers are expected to handle these exceptions, otherwise your data will be lost.
    Example of transient failure:
        -NotEnoughReplicasException
        
    ==> There is a "retries" setting
        - defaults to 0 to Kafka Version <=2
        - defaults to 2147483647 to Kafka Version >=2.1
        
    ==> The 'retry.backoff.ms' settings is by default 100ms
    
    ### Producer Retries: Warning
        -   In case of retries, there's a chance that messages will be sent out of order.
            For example, if a batch has failed to be sent.
        - So if you rely on key-based ordering, that can be a really big issue,
         
    Solution for this is idempotent producers
## Producer Timeouts
    - If retries > 0, the producer will not rety for ever, it's bounded by timeout
    - delivery.timeout.ms = 120000ms == 2 min
    
# Idempotent Producer
##Problem: 
Producer can introduce duplicate messages in Kafka due to network error
##Solution:
In Kafka >= 0.11, you can define a "idempotent producer" which won't introduce duplicate dut to n/w error
##Enable Idempotent Producer
    producerProps.put("enable.idempotence", true);
    
    Q: What it does?
    A: Sets below properties by default:
        -> retries = INteger.MAX_VALUE(2^31-1 = 2147483647)
        -> max.in.flight.requests = 1 (Kafka == 0.11) or
        -> max.in.flight.requests = 2 (Kafka >= 1.0 - higher performance & keep ordering)
        -> asks=all
        
 #Producer Compression
    https://blog.cloudflare.com/squeezing-the-firehose/ 
        