package com.github.fiona.kafka.tutorial1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

/*
* Consumer demo with thread - able to close consumer when the Main thread exists.
* */
public class ConsumerDemoWithThread {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    public static void main(String[] args) {
        ConsumerDemoWithThread consumerDemoWithThread = new ConsumerDemoWithThread();
        consumerDemoWithThread.run();
    }

    public ConsumerDemoWithThread() {}

    public void run() {
        String bootstrapServer = "localhost:9092";
        String groupId = "my_fifth_application";
        String topic = "first_topic";

        // Create runnable
        CountDownLatch latch = new CountDownLatch(1); // the number of invited thread
        Runnable consumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch
        );

        // start thread
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        // create shutdown hook
        // plugin a piece of code that will be executed when the program is shutting down
        // The hook will start a thread that will start running at the time of termination
        // Why? - clean up, logging, after-error actions, send a signal to another process
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            // should log when user exit the program
            this.logger.info("Caught showdown event ");
            ((ConsumerRunnable)consumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            this.logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            this.logger.error("Application got interrupted");
        } finally {
            this.logger.info(" Application is closing");
        }
    }
}

