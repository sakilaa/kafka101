package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    //psvm shortcut
    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();



    }
    private  ConsumerDemoWithThread(){


    }
    private  void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupID = "my-six-application";
        String topic ="first_topic";

        //latch for dealing with multiple thread
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating the consumer Thread ");
        //create the consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupID,
                topic,
                latch);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Application Got interrupted ", e);
                //e.printStackTrace();
            } finally {
                logger.info("Application is Closing");
            }
        }
        ));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application Got interrupted ", e);
            //e.printStackTrace();
        } finally {
            logger.info("Application is Closing");
        }

    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer ;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        // Deals with concurrency in Java using CountDownLatch , This latch is used to shutdown application correctly
        public ConsumerRunnable(
                              String bootstrapServers,
                              String groupID,
                              String topic,
                              CountDownLatch latch){
            this.latch= latch;
            //creates consumer Configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earlest , latest , none are the properties

            KafkaConsumer<String ,String > consumer= new KafkaConsumer<String, String>(properties);
        }

        @Override
        public void run() {
            try{
            while (true) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key" + record.key() + ", Value :" + record.value() );
                    logger.info("Partition:" + record.partition() + ", Offset " + record.offset());
                }

            }} catch (WakeupException e){
            logger.info(("Received Shutdown signal "));
        } finally {
                consumer.close();
                //tell our main code  we are done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //wakeup method is a special method to interrupt consumer.poll()
            //it will throw the exception - WakeUpException
            consumer.wakeup();
        }
    }

}
