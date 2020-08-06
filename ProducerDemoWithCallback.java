package com.anirudh.testcode.firstCode;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        //Create Producer Properties

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServer="127.0.0.1:9092";

        Properties properties = new Properties();
        //See Kafka Documentation

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Old way
       // properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());


        //Create the producer
        //For Key and Value to be string
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Send data

        //Topic in below is hint and not any code
        for (int i=10 ;i<20 ;i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("First_Topic", "New Hello World " + Integer.toString(i));


            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown


                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");

                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        //flush Data

        producer.flush();
        producer.close();
    }
}
