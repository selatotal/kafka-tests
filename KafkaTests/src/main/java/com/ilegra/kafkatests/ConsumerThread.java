package com.ilegra.kafkatests;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * Created by talesviegas on 08/11/2017.
 */
public class ConsumerThread extends Thread{

    KafkaConsumer<String, String> kafkaConsumer;

    ConsumerThread(KafkaConsumer<String, String> kafkaConsumer){
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void run(){
        kafkaConsumer.subscribe(Arrays.asList(SimpleProducer.TOPIC_NAME));

        System.out.println("Subscribed to topic: " + SimpleProducer.TOPIC_NAME);

        long i = 0;
        while(true){
            System.out.println("Reading records...");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            }
        }
    }
}
