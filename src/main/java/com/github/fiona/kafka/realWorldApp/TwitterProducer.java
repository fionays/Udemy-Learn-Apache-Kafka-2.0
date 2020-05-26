package com.github.fiona.kafka.realWorldApp;

import com.google.common.collect.Lists;
import com.sun.jdi.BooleanType;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private String consumerKey="key";
    private String consumerSecret="cSecret";
    private String token="token";
    private String secret="secret";


    private String topic = "twitter_tweets";
    private int partition = 6;
    List<String> terms = Lists.newArrayList("kafka", "fun", "learning");

    public static void main(String[] args) {
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    public TwitterProducer() {}

    public void run() {
        logger.info("Set up");
        // Step 1: create a twitter client. https://github.com/twitter/hbc
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client twitterClient = createTwitterClient(msgQueue);

        // Step 2: create a producer
        KafkaProducer<String, String> twitterProducer= createTwitterProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            twitterClient.stop();
            twitterProducer.close();// send out the messages in memory before the app is shutting down
            logger.info("done!");
        }));
        while (!twitterClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop(); //close the connection
            }
            if(msg !=null ) {
                logger.info(msg);
                // Note: cannot produce to a topic that does not exist, so, go to console to create a topic first
                twitterProducer.send(new ProducerRecord<String, String>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Something bad happened!", e);
                        }
                    }
                });
            }
        }
        this.logger.info("End of application");

    }

    public Client createTwitterClient(BlockingQueue msgQueue) {

        // Declaring the connection information
        /*Declare the host you want to connect to, the endpoint, and auth*/
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Set up some track terms
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Creating a client
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client twitterClient = builder.build();
        // Attempts to establish a connection
        twitterClient.connect();

        // taking data on a different thread, or multiple different threads...
        return twitterClient;
    }

    public KafkaProducer<String, String> createTwitterProducer() {
        // bootstrap server
        String bootstrapServer = "localhost:9092";
        // Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create safe Producer
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // The following are not needed to set but it is always good to set them explicitly
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // high throughput producer (at the expense of a bit of lag and CPU usage)
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB batch size

        KafkaProducer producer = new KafkaProducer(props);
        return producer;
    }
}
