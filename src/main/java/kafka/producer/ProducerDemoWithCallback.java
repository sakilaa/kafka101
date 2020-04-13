package kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    //psvm shortcut
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
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
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic" , "HelloWorld1 ");
        //Send data -- Asynchronus
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes everytime a record is successfully send or exception is thrown
                if (e == null) {
                    // record send successfully.
                    logger.info("Recieved new metadata \n" +
                            "Topic:"  + recordMetadata.topic() + "\n" +
                            "Partition:" + recordMetadata.partition() + "\n" +
                             "offset:"  + recordMetadata.offset() +"\n" +
                             "TimeStamp:" + recordMetadata.timestamp()) ;
                }
                else {
                    logger.error("Error while Producing" , e);

                }
            }
        });

        //Flush data
        producer.flush();
        //flush and close 
        producer.close();


    }

}
