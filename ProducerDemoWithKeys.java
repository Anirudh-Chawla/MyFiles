package com.anirudh.testcode.firstCode;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //Create Producer Properties

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

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
        for (int i=0 ;i< 10 ;i++) {

            String topic = "First_Topic";
            String value = "Hello Kafka" +Integer.toString(i);
            String key = "id_" + Integer.toString(i);


            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key" + key); //Log the key


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
            }).get();

            // Block the .send() to make it synchronous but dont do it production
        }

        //flush Data

        producer.flush();
        producer.close();
    }

}


//Key id_0 - 1
//Keyid_1 - 0
//id2 - 2
//id3 - 0
//id4-2
//id5-2
//id6-0
//id7-2
//id8-1
//id9-2
//