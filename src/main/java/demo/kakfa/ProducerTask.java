package demo.kakfa;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerTask implements Callable<Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerTask.class);
    private int producerId;
    private ConcurrentMap<String, Object> controlMap;
    private String topic;
    private KafkaProducer<String, String> producer;
    private Long nMessagesTarget;
    private AtomicLong totalMessagesCount;

    // KafkaProducer is thread-safe, but has single background IO thread.
    // Hence kept per-thead to allow more IO concurrency
    private KafkaProducer<String, String> kafkaProducer;

    public ProducerTask(Integer producerId, ConcurrentMap<String, Object> controlMap) {
        this.controlMap = controlMap;
        this.producerId = producerId;
        this.topic = (String) controlMap.get("topic-name");
        this.nMessagesTarget = (Long) controlMap.get("target");



        final Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        setupReliableProductionSetting(props);

        this.producer = new KafkaProducer<String, String>(props);

        totalMessagesCount = (AtomicLong) this.controlMap.get("total-messages");
        if (totalMessagesCount == null) {
            totalMessagesCount = new AtomicLong(0); // just for the rest of the code to be safe
        }

    }

    private void setupReliableProductionSetting(Properties props) {
        /**  Properties for reliable at-least-once processing -- START */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) controlMap.get("broker"));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "0");
        /**  Properties for reliable at-least-once processing -- END */
    }

    private boolean sendMessage(String id) throws Exception {
        Event.EventBuilder eventBuilder = new Event.EventBuilder();
        // correlation id
        if (id == null) {
           id = UUID.randomUUID().toString();
        }
        // static event data  for now
        final Event e = eventBuilder
                .withName("sales")
                .withPrice((long) 10)
                .withTax((long) 1)
                .build();
        final ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, id, e.toJson());

        // add the correlation id to headers
        record.headers().add(new RecordHeader("id", id.getBytes()));


        //send record
        Future<RecordMetadata> future = this.producer.send(record);
        //make durable
        this.producer.flush();
        LOGGER.info("message {} sent\n", id);
        //get results
        return extractSendResultAndLog(record, future);
    }

    public Long call() throws Exception {
        long i;
        String id = UUID.randomUUID().toString() ;
        for (i = 0; i < this.nMessagesTarget; i++) {
            if (i%2 != 0) {
                id = UUID.randomUUID().toString();
                // send duplicate messages on purpose
            }
            if (this.sendMessage(id) ) {
                totalMessagesCount.getAndIncrement();
            }
            if ("true".equals(controlMap.getOrDefault("stop-producing", "false"))) {
                System.out.printf("\tStopping thread %d , messages so far", i);
                return i;
            }
            Thread.sleep(100);
        }

        return i;
    }

    private boolean extractSendResultAndLog(ProducerRecord<String, String> record, Future<RecordMetadata> future) {
        RecordMetadata sendResult = null;

        try {
            sendResult = future.get(); // even if the producer returns FutureFailure, the get will throw the original exception
            if (sendResult != null) { // log only if there is send result is not null (will happen in case of adding to cache)
                LOGGER.info("\t\tSend to kafka successful, topic - {}, partition - {}, offset - {}",
                        sendResult.topic(), sendResult.partition(),
                        sendResult.offset());
                return true;
            }
        } catch (ExecutionException e) {
            LOGGER.error("ExecutionException while kafka send to topic - {}", record.topic(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("InterruptedException while kafka send to topic - {}", record.topic(), e);
        } catch (Exception e) {
            LOGGER.error("Error in send msg  to topic - {}", record.topic()); //TODO - add more info?
        }

        return false;
    }
}
