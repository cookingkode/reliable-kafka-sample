package demo.kakfa;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaContainer extends Thread {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaContainer.class);
    private KafkaConsumer<String, String> consumer; // NOT thread-safe
    private ConcurrentMap<String,Object> controlMap;
    // kafka poll timeout
    private Duration pollTimeout;
    public enum WorkResponse {
        SUCCESS, FAILURE, DISCARD;
    }

    private AtomicLong totalMessagesCount;

    public KafkaContainer( List<String> topics, ConcurrentMap<String, Object> controlMap ) {
        this.controlMap = controlMap;

        // create a Kafka consumer
        final Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, controlMap.getOrDefault("groupId", "reliable-cons"));
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, controlMap.getOrDefault("broker","localhost:9092"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        /**  Properties for reliable at-least-once processing -- START */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // max.poll.records - maximum number of records returned in a single call to poll()
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, controlMap.getOrDefault("maxPollRecords",1)); // TODO - check if/how maxPollRecords is used in 2.3.x
        // max.poll.interval.ms - what is the maximum time spent in processing messaging
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, controlMap.getOrDefault("maxPollIntervalMs",5000));
        // session.timeout.ms - what is the time by which broker will timeout this consumer
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, controlMap.getOrDefault("sessionTimeoutMs",10000));
        // how often will the consumer  heartbeat - should not be more than session.timeout.ms /3
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, controlMap.getOrDefault("heartbeatIntervalMs",1000));
        /**  Properties for reliable at-least-once processing -- END */
        this.consumer = new KafkaConsumer<String, String>(props);

        this.pollTimeout = Duration.ofMillis((Long) controlMap.getOrDefault("pollTimeout", 100L));
        this.consumer.subscribe(topics);

        totalMessagesCount = (AtomicLong) this.controlMap.get("total-messages");
        if (totalMessagesCount == null) {
            totalMessagesCount = new AtomicLong(0); // just for the rest of the code to be safe
        }
    }

    @Override
    public void run() {
        while (!closed.get()) {
            ConsumerRecords<String, String> records = consumer.poll(this.pollTimeout);
            records.forEach(record -> {
                LOGGER.debug("Received" + getMessageDetailsforLogging(record));
                try {
                    String id = record.headers().lastHeader("id").value().toString();
                    Event event = new com.google.gson.Gson().fromJson(record.value(), Event.class);
                    WorkResponse response = handleWork(id, event);
                    handleWorkResponse(response, id, record);
                } catch (Exception e) {
                    LOGGER.error("error error while kafka deserialization - " + e.getMessage());
                    LOGGER.error("message details - " + getMessageDetailsforLogging(record));
                }
            });
        }
    }

    private WorkResponse handleWork(String Id, Event e) {
        // just print
        LOGGER.info("event received Id : {} ", Id);

        //TODO
        return WorkResponse.SUCCESS;
    }


    private void handleWorkResponse(WorkResponse workResponse, String id, ConsumerRecord<String, String> record) {
        if (workResponse == null) {
            workResponse = WorkResponse.FAILURE;
            LOGGER.warn("unable to determine success or failure of doWork method, work handler "
                    + "returned null response for message id: {}", record.key());
        }

        LOGGER.debug("workResponse: {},", workResponse);

        switch (workResponse) {
            case SUCCESS:
                //  flush senders - TODO
                totalMessagesCount.getAndIncrement();
                break;
            case DISCARD:
                sendDiscardedMsgToBadTopic(record);
                break;
            case FAILURE:

                sendDiscardedMsgToBOTopic(record);
        }

        // In all cases - commit offset
        consumer.commitSync();
    }

    private void sendDiscardedMsgToBadTopic(ConsumerRecord<String, String> record) {
        // TODO
    }

    private void sendDiscardedMsgToBOTopic(ConsumerRecord<String, String> record) {
        // TODO
    }

    private String getMessageDetailsforLogging(ConsumerRecord<String, String> record) {
        return String.format("kafka message topic %s partition %d key %s offset %d value %s",
                record.topic(),
                record.partition(),
                record.key(),
                record.offset(),
                record.value()
        );

    }

}