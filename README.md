# reliable-kafka-sample
Kafka needs special tuning to perform reliable at-least-once processing.
This sample code demonstrates  producer/consumer for reliable at-least-once processing

## Producer
Reliable production  includes scenarios like  thread restarts, just after ``KafkaProducer.send()`` .  These tuneables 
are meant to *guarantee* that write on kakfa is durable over more than 1 node

The producer ensures that when a message is deemed 'sent ok', it is available on at-least ```min_isr``` number of nodes. 
This needs the ```acks=all``` setting as well. Please see ```ProducerTask.setupReliableProductionSetting``` for the full
set of producer settings.

Besides these settings, key aspects of safe production include
-  ```min.insync.replicas```  must be at least 2 ( for single node failure tolerance. 
It can be set at a cluster or at a topic level
- After a ```KafkaProducer.send()``` , we need to ```flush()``` and do an ```get()``` on the future returned
This ensures that the write is available on broker before the application 
assumes "write done"

## Consumer
Kadka maintains two 'offsets' for each TopicParitions
- Read offset : This gives the offset of the next record that will be read by the consumer. It advances on very ```poll()```
- Commit offset :  This is the last offset that has been stored as 'processed' on the brokers. If the process/threds fails
/restarts then this is the offset that the consumer will recover from.
 
A reliable consumer means that message offset in a TopicPartition is committed only after the message has been processed.
In case the message cannot be processed (due to some errors or unavailability of dependent systems), the message must be 
parked in a retry ("bo") or dead-letter ("bad") topic.  Note : These auxilllary topics are per-consumer since different 
consumers will treat the same message differently

A key tuneable here is  ```enable.auto.commit``` . If this is True, it means that offsets are committed automatically 
at a frequency defined by another  config ```auto.commit.interval.ms```. For higher reliability/control, this is set to 
False and manual control done using ```commitSync()``` . For example of what can go wrong, see the following note from 
the kafka documentation
``
Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that you must
consume all data returned from each call to poll(long) before any subsequent calls, or before closing the consumer.
If you fail to do either of these, it is possible for the committed offset to get ahead of the consumed position, which
 results in missing records. The advantage of using manual offset control is that you have direct control over when a 
record is considered "consumed"``
 
Another couple of tuneables which affect consumer behavior are ```max.poll.interval.ms``` and  ```max.poll.records```. 
These together define how much time consumer will take to respond to broker - so that consumer health can be  guaged
 . Generally setting the record to 1 and figuring out max time to process 1 record as ```max.poll.interval.ms```  . If 
 the time between poll() crosses this threshold, then the  consumer will proactively leave the group.




Please see ```ConsumerTask.setupReliableConsumptionSetting``` for the full list of consumer settings.

In addition, the consumer demonstrates how to achieve idempotency using the 'id' value in the Kafka message header



## Usage

- Many tunebales ( brokerstring, topic, etc) are defined in ```application.properties```

- To run the Producer , use this on the command line
``java -Dserver.port=9090 -jar target/reliable-kafka-sample-1.0-SNAPSHOT.jar  -producer``

- To run the Consumer , use this on the command line
``java -Dserver.port=9090 -jar target/reliable-kafka-sample-1.0-SNAPSHOT.jar  -consumer``

- For the long-running consumer, one can navitage to ```http://localhost:8080/h2-console/``` to get an UI interface for 
the events DB.
