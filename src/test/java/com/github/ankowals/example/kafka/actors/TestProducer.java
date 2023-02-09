package com.github.ankowals.example.kafka.actors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer<K, V> {

    private final KafkaProducer<K, V> kafkaProducer;

    TestProducer(KafkaProducer<K, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void produce(String topic, V value) {
        send(createRecord(topic, value));
    }

    public void produce(String topic, K key, V value) {
        send(createRecord(topic, key, value));
    }

    public void produce(ProducerRecord<K, V> producerRecord) {
        send(producerRecord);
    }

    public void produceAndClose(String topic, V value) {
        produce(topic, value);
        close();
    }

    public void produceAndClose(String topic, K key, V value) {
        produce(topic, key, value);
        close();
    }

    public void produceAndClose(ProducerRecord<K, V> producerRecord) {
        produce(producerRecord);
        close();
    }

    private void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
    }

    public void close() {
        kafkaProducer.close();
    }

    private ProducerRecord<K, V> createRecord(String topic, V value) {
        return new ProducerRecord<>(topic, value);
    }

    private ProducerRecord<K, V> createRecord(String topic, K key, V value) {
        return new ProducerRecord<>(topic, key, value);
    }
}
