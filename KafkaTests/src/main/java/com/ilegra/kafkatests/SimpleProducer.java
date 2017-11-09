package com.ilegra.kafkatests;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {

    public static final String TOPIC_NAME = "kafkatest";

    public static void main(String[] args){


        Properties propsProducer = new Properties();
        Properties propsConsumer = new Properties();
        propsProducer.put("bootstrap.servers", "localhost:9092");
        propsProducer.put("acks", "all");
        propsProducer.put("retries", 0);
        propsProducer.put("batch.size", 16384);
        propsProducer.put("linger.ms", 1);
        propsProducer.put("buffer.memory", 33554432);
        propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        propsConsumer.put("bootstrap.servers", "localhost:9092");
        propsConsumer.put("enable.auto.commit", "true");
        propsConsumer.put("auto.commit.interval.ms", "1000");
        propsConsumer.put("session.timeout.ms", "30000");
        propsConsumer.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        propsConsumer.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(propsProducer);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(propsConsumer);

        ProducerThread producerThread = new ProducerThread(producer, 50);
        ConsumerThread consumerThread = new ConsumerThread(consumer);

        producerThread.start();
        consumerThread.start();


    }
}
