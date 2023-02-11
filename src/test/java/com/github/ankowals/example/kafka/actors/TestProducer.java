package com.github.ankowals.example.kafka.actors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer<K, V> {

    private final String topic;
    private final KafkaProducer<K, V> kafkaProducer;

    TestProducer(String topic, KafkaProducer<K, V> kafkaProducer) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    public void send(V value) { send(createRecord(value)); }

    public void send(K key, V value) {
        send(createRecord(key, value));
    }

    private void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
    }

    public void produce(V value) {
        send(value);
        close();
    }

    public void produce(K key, V value) {
        send(key, value);
        close();
    }

    public void produce(ProducerRecord<K, V> producerRecord) {
        send(producerRecord);
        close();
    }

    public void close() {
        kafkaProducer.close();
    }

    private ProducerRecord<K, V> createRecord(V value) {
        return new ProducerRecord<>(this.topic, value);
    }

    private ProducerRecord<K, V> createRecord(K key, V value) {
        return new ProducerRecord<>(this.topic, key, value);
    }
}
