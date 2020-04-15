package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroup {
    //psvm shortcut
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroup.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        //By changing the group We can read the messages again from the same topic without changing the offset
        String groupID = "my-fifth-application";
        String topic ="first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earlest , latest , none are the properties

        //Create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);


        //Subscribe consumer to out Topics
        //We   can do multiple  topic as consumer.subscribe(Arrays.asList("First_topic", "second_topic" )) ;
        consumer.subscribe(Collections.singleton(topic));
        //consumer.subcribe(Arrays.asList(topic));

        //Poll data

        while (true) {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key" + record.key() + ", Value :" + record.value() );
                logger.info("Partition:" + record.partition() + ", Offset " + record.offset());
            }

        }

    }

}
