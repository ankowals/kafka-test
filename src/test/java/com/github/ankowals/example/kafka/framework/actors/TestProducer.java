package com.github.ankowals.example.kafka.framework.actors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer<K, V> {

    private final String topic;
    private final KafkaProducer<K, V> kafkaProducer;

    public TestProducer(String topic, KafkaProducer<K, V> kafkaProducer) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    public void send(V value) { this.send(this.createRecord(value)); }

    public void send(K key, V value) {
        this.send(this.createRecord(key, value));
    }

    private void send(ProducerRecord<K, V> producerRecord) {
        this.kafkaProducer.send(producerRecord);
        this.kafkaProducer.flush();
    }

    public void produce(V value) {
        this.send(value);
        this.close();
    }

    public void produce(K key, V value) {
        this.send(key, value);
        this.close();
    }

    public void produce(ProducerRecord<K, V> producerRecord) {
        this.send(producerRecord);
        this.close();
    }

    public void close() {
        this.kafkaProducer.close();
    }

    private ProducerRecord<K, V> createRecord(V value) {
        return new ProducerRecord<>(this.topic, value);
    }

    private ProducerRecord<K, V> createRecord(K key, V value) {
        return new ProducerRecord<>(this.topic, key, value);
    }
}
