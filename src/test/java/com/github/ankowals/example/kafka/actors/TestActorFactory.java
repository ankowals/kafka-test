package com.github.ankowals.example.kafka.actors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import java.util.Properties;

public class TestActorFactory {

    private final String bootstrapServer;

    public TestActorFactory(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public CustomTestConsumer<String, String> createConsumer() {
        Properties properties = consumerProperties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new CustomTestConsumer<>(new KafkaConsumer<>(properties));
    }

    public CustomTestConsumer<String, String> createConsumer(String keyDeserializerClassName, String valueDeserializerClassName) {
        Properties properties = consumerProperties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);

        return new CustomTestConsumer<>(new KafkaConsumer<>(properties));
    }

    public KafkaProducer<String, String> createProducer() {
        Properties properties = producerProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaProducer<>(properties);
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testConsumersActors");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + english(8));

        return properties;
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        return properties;
    }

    private String english(int length) {
        return RandomStringUtils.randomAlphabetic(length);
    }

}
