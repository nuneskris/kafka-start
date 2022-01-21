package com.study.nuneskris.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.lang.Integer;

import java.util.Properties;

public class ProducerHello {
    Logger logger = LoggerFactory.getLogger(ProducerHello.class);
    public static void main(String args[]){
        ProducerHello myp = new ProducerHello();
        myp.process();
    }

    private void process(){

        // producer config
        Properties pros = new Properties();
        pros.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        pros.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pros.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(pros);

        // produce a record
        String topic = "first_topic";
        for(int i =0 ; i < 10 ; i++) {

            String key = "ID_"+Integer.toString(i);
            String value = "hellofrom-java" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key ,value);
            send(producer, record);
        }
        producer.flush();
        producer.close();

    }
    private void send( KafkaProducer<String, String> producer,  ProducerRecord<String, String> record ){
        // send some data

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("Received new metadata,"+recordMetadata.topic()+","
                            + recordMetadata.partition() + ","
                            + record.key() + ","
                            + recordMetadata.offset());
                }else{
                    e.printStackTrace();
                }
            }
        });
    }
}
