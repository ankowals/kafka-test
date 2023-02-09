package com.github.ankowals.example.kafka.actors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

public class TestActorFactory {

    private final String bootstrapServer;

    public TestActorFactory(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public <K, V> TestConsumer<K, V> createConsumer(Class<? extends Deserializer> keyDeserializerClass, Class<? extends Deserializer> valueDeserializerClass) {
        Properties properties = consumerProperties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());

        return new TestConsumer<>(new KafkaConsumer<>(properties));
    }

    public <K, V> TestProducer<K, V> createProducer(Class<? extends Serializer> keySerializerClass, Class<? extends Serializer> valueSerializerClass) {
        Properties properties = producerProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());

        return new TestProducer<>(new KafkaProducer<>(properties));
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testConsumersActors");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + randomAlphabetic(8));

        return properties;
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + randomAlphabetic(8));

        return properties;
    }
}
