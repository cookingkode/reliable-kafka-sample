package demo.kakfa;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Driver {
    private static final String TOPIC = "test";
    private static final int numTasks = 5;
    final static Logger LOGGER = LoggerFactory.getLogger(Driver.class);

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        // create Options object
        Options options = new Options();
        // add t option
        options.addOption("producer", false, "start a kafka producer");
        options.addOption("consumer", false, "start a kafka consumer");
        options.addOption("brokers", true, "kafka brokers to connect");
        CommandLineParser parser = new DefaultParser();

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "ant", options );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            String broker = line.getOptionValue("brokers", "localhost:9092");
            if (line.hasOption("producer")) {
                producerDriver(broker);
            } else {
                consumerDriver(broker);
            }
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }


    }

    /** Kafka message producer*/
    public static void producerDriver(String broker){
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
            thisTasksMap.put("broker", broker);
            tasks.add(new ProducerTask(i, thisTasksMap));
            controlMaps.add(i, thisTasksMap);
        }

        Long totalMessagesSent = new Long(0);

        // Execute
        try {
            List<Future<Long>> result = new ArrayList<>(); // .stream().collect(Collectors.toList());
            for (Callable<Long> c : tasks) {
                result.add(executor.submit(c));
            }
            LOGGER.info("Tasks started");
            totalMessagesSent = result.stream()
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

        LOGGER.info("Total messages sent : ", totalMessagesSent);
        executor.shutdown();
    }

    /** Kafka message consumer*/
    public static void consumerDriver(String broker) {
        ConcurrentMap consControlMap = new ConcurrentHashMap<String, Object>();

        CountDownLatch latch = new CountDownLatch(1);
        consControlMap.put("broker", broker);
        KafkaContainer theContainer = new KafkaContainer(Arrays.asList(TOPIC),consControlMap);
        theContainer.start();
        try {
            latch.await(); // TODO wait indefintely
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}


