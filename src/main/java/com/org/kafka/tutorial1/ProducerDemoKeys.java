package com.org.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger=LoggerFactory.getLogger(ProducerDemoWithCallback.class);


        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer .class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++){

            String topic="first_topic";
            String value="hello world"+Integer.toString(i);
            String key="id_"+Integer.toString(i);


            ProducerRecord<String,String> record=new ProducerRecord<String,String>(topic,key,value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if(e==null)
                    {
                        //the record was successfully sent
                        logger.info("Received new meta data. \n"+"Topic: " +recordMetadata.topic()+"\n"+
                                "Partition: "+recordMetadata.partition()+"\n"+
                                "Offset: "+recordMetadata.offset()+"\n"+
                                "Timestamp: "+record.timestamp());

                    }
                    else {
                        logger.error("Error while producing",e);
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }

}
