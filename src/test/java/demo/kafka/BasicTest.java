package demo.kafka;

import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import demo.kakfa.ConsumerTask;
import demo.kakfa.ProducerTask;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class BasicTest {
    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
            // Start a cluster with 2 brokers.
            .withBrokers(3);



    @Test
    public void testSendAndReceive() throws Exception {
        final String topicName = "test-kafka-demo-ut";

        //Send
        ConcurrentMap producerControlMap = new ConcurrentHashMap<String, Object>();
        producerControlMap.put("topic-name", topicName);
        producerControlMap.put("total-messages", new AtomicLong(0));
        producerControlMap.put("broker", sharedKafkaTestResource.getKafkaConnectString());
        ProducerTask producerTask = new ProducerTask(1, producerControlMap);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(producerTask);

        //Receive
        ConcurrentMap consControlMap = new ConcurrentHashMap<String, Object>();
        consControlMap.put("broker", sharedKafkaTestResource.getKafkaConnectString());
        consControlMap.put("total-messages", new AtomicLong(0));
        consControlMap.put("dropped-messages", new AtomicLong(0));
        ConsumerTask theContainer = new ConsumerTask(Arrays.asList(topicName), null,consControlMap);
        theContainer.start();

        AtomicLong totalProducerMessages =  (AtomicLong)producerControlMap.get("total-messages");
        AtomicLong totalConsumerMessages =  (AtomicLong)consControlMap.get("total-messages");

        Thread.sleep(10000);
        assertEquals(totalProducerMessages.longValue(),totalConsumerMessages.longValue());


    }

}

