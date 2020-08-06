package com.anirudh.testcode.firstCode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();

    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){
         Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        String bs = "127.0.0.1:9092";
        String groupid="newcodegroup3";

        String topic = "First_Topic";


        //Latch for Dealing with Multiple threads
        CountDownLatch latch = new CountDownLatch(1);


        //Create the consumer Runnable
        logger.info("Creating consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch,topic,bs,groupid);

        //Start the thread
        Thread mythread = new Thread(myConsumerRunnable);
        mythread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught Shutdown Hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }



        ));



        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("App got interrupted");
        }finally {
            logger.info("App is closing");
        }

    }
    public class ConsumerRunnable implements  Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


        public ConsumerRunnable(CountDownLatch latch, String topic, String bs, String groupid){
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bs);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            consumer = new KafkaConsumer<String, String>(properties);

            //consumer.subscribe(Collections.singleton(topic));

            //To Subscribe to Multiple Topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            try {
                while (true) {

                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition() + " Offset: " + record.offset());

                    }
                }
            }
            catch (WakeupException e){
                //Logger
            logger.info("Received Shutdown Signal");
            }
            finally{
                consumer.close();
                //Tell our maincode we are done
                latch.countDown();
            }

        }
        public void shutdown(){
            //Wakeup method is a special method to interuupt consumer.poll()
            //it will throw the wakeup exceptiom
            consumer.wakeup();
        }
    }
}
