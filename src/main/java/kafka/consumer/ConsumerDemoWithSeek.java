package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithSeek {
    //psvm shortcut
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithSeek.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        //String groupID = "my-seventh-application";
        String topic ="first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earlest , latest , none are the properties

        //Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // assign and seek are mostly used to replay data or fetch a specific message
        // so we will not have a group id and will not subscribe to consumer topic
        //assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        //Subscribe consumer to out Topics
        //We   can do multiple  topic as consumer.subscribe(Arrays.asList("First_topic", "second_topic" )) ;
        //consumer.subscribe(Collections.singleton(topic));
        //consumer.subcribe(Arrays.asList(topic));

        int numberofMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberofMessagesReadSoFar = 0;
        //Poll data

        while (keepOnReading) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberofMessagesReadSoFar +=1 ;
                logger.info("Key" + record.key() + ", Value :" + record.value() );
                logger.info("Partition:" + record.partition() + ", Offset " + record.offset());
                if (numberofMessagesReadSoFar >= numberofMessagesToRead) {
                    keepOnReading=false; //exit the for loop
                    break; // to exit before loop
                }
            }

        }
        logger.info("Existing application ");
    }

}
