package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    //psvm shortcut
    public static void main(String[] args) {

        System.out.println("Test project");

        String bootStrapServers = "127.0.0.1:9092";

        // To create producer properties

        Properties properties = new Properties();
        //https://kafka.apache.org/documentation/#producerconfigs
        //set only required properties
        // This is the old way of doing now , see below
        /*properties.setProperty("bootstrap.servers",bootStrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());

        properties.setProperty("value.serializer",StringSerializer.class.getName());
        */
        properties.setProperty((ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());




        //To create the producer


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic" , "HelloWorld");
        //Send data -- Asynchronus
        producer.send(record);

        //Flush data
        producer.flush();
        //flush and close
        producer.close();


    }

}
