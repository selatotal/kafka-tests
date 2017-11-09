package com.ilegra.kafkatests;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;

public class ProducerThread extends Thread {

    Producer<String, String> producer;
    long numMessages;

    ProducerThread(Producer<String, String> producer, long numMessages){
        this.producer = producer;
        this.numMessages = numMessages;
    }

    @Override
    public void run(){
        for(long i=0; i < numMessages; i++){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(SimpleProducer.TOPIC_NAME, ""+i, ""+i);
            producer.send(producerRecord);
            System.out.println("Message " + i + " sent successfully - " + new Date() );

        }

        producer.close();
    }

}
