package com.org.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
       //create the producer properties
        Properties properties=new Properties();
        //properties.setProperty("bootstrap.servers","localhost:9092");
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());
        // key serializer and value serializer helps to know what type of value you are sending to kafka and how this should be serialized to byte.
        //because kafka would convert whatever kafka client send to kafka into kafka bytes(0s and 1s). Since we will send string so we need to find
        // out the key serializer for string and value serializer for strings.
        // the above was old method to set properties the new method is given below.

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String,String> record=new ProducerRecord<String,String>("first_topic","helloAkshya");
        // send data-asynchronous (multiple tasks can run in parallel)
        producer.send(record);// so before it could produce the data main method exits
        // to wait for data to be produced
        producer.flush();// this force all data to be producer
        //flush and close producer
        producer.close();
    }
}
