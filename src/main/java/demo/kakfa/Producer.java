package demo.kakfa;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Producer {
    private static final String TOPIC = "test";
    private static final int numTasks = 5;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {
        final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        ExecutorService executor = Executors.newFixedThreadPool(numTasks);
        List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();

        // Prep
        List<ConcurrentMap<String, Object>> controlMaps = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            ConcurrentMap thisTasksMap = new ConcurrentHashMap<String, Object>();
            thisTasksMap.put("test_key", "test_val"); // just a test
            thisTasksMap.put("target", new Long(2)); // number of messages to send
            //thisTasksMap.put("target", Math.round(Math.random() * 10)); // number of messages to send
            thisTasksMap.put("topic-name", TOPIC);
            tasks.add(new ProducerTask(i, thisTasksMap));
            controlMaps.add(i, thisTasksMap);
        }

        Long totalMessages = new Long(0);

        // Execute
        try {
            List<Future<Long>> result = new ArrayList<>(); // .stream().collect(Collectors.toList());
            for (Callable<Long> c : tasks) {
                result.add(executor.submit(c));
            }

            LOGGER.info("Tasks started");

            totalMessages = result.stream()
                    .map(future -> {
                        Long res = new Long(0);
                        try {
                            res = (Long) future.get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return res;
                    })
                    .collect(Collectors.summingLong(Long::longValue));


        } catch (Exception e) {
            e.printStackTrace();
        }


        System.out.printf("Total messages sent : %d\n", totalMessages);
        executor.shutdown();

    }
}
