package demo.kakfa;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


@SpringBootApplication
public class Driver implements CommandLineRunner {
    final static Logger LOGGER = LoggerFactory.getLogger(Driver.class);

    @Autowired
    private EventService eventService;

    @Value("${kafka.consumer.nthreads}")
    private int numConsumers;

    @Value("${kafka.topic.name}")
    private String topic;

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.producer.nthreads}")
    private int numProducers;


    public static void main(String[] args) {
        SpringApplication.run(Driver.class, args);
    }

    @Override
    public void run(String... argsList) {
        this.driver(argsList);
    }
    public  void driver(final String[] args) {
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

            String broker = line.getOptionValue("brokers", brokers); // override props from cmd line
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
    public  void producerDriver(String broker){
        ExecutorService executor = Executors.newFixedThreadPool(numProducers);
        List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();

        // Prep
        List<ConcurrentMap<String, Object>> controlMaps = new ArrayList<>();
        for (int i = 0; i < numProducers; i++) {
            ConcurrentMap prodControlMap = new ConcurrentHashMap<String, Object>();
            prodControlMap.put("test_key", "test_val"); // just a test
            prodControlMap.put("target", new Long(4)); // number of messages to send
            prodControlMap.put("total-messages", new AtomicLong(0));
            prodControlMap.put("topic-name", topic);
            prodControlMap.put("broker", broker);
            tasks.add(new ProducerTask(i, prodControlMap));
            controlMaps.add(i, prodControlMap);
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

        LOGGER.info("Total messages sent : {} ", totalMessagesSent);
        executor.shutdown();
    }

    /** Kafka message consumer*/
    public  void consumerDriver(String broker) {
        List<ConcurrentMap<String,Object>> concurrentMaps = new ArrayList<>();
        int i;
        for (i=0; i<numConsumers; i++) {
            ConcurrentMap consControlMap = new ConcurrentHashMap<String, Object>();
            consControlMap.put("broker", broker);
            consControlMap.put("total-messages", new AtomicLong(0));
            consControlMap.put("dropped-messages", new AtomicLong(0));
            concurrentMaps.add(consControlMap);

            KafkaContainer theContainer = new KafkaContainer(Arrays.asList(topic), this.eventService, consControlMap);
            theContainer.start();
        }

        while (true) {
            try {
                Thread.sleep(1000);
                Long totalMessages = 0L;
                for (i=0; i<numConsumers; i++){
                    totalMessages += ((AtomicLong)
                            concurrentMaps.get(i).getOrDefault("total-messages",new AtomicLong(0))).get();
                }

                //LOGGER.info("Total messages consumed = {}\n", totalMessages);

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


    }

}


