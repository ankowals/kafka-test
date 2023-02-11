package com.github.ankowals.example.kafka.actors;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

public class TestActorFactory {

    private final Properties properties;

    public TestActorFactory(String bootstrapServer) {
        this(bootstrapServer, "");
    }

    public TestActorFactory(String bootstrapServer, String schemaRegistryUrl) {
        this.properties = new Properties();
        this.properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        setConsumerProperties();
        setProducerProperties();
    }

    public TestActorFactory(Properties properties) {
        this.properties = properties;
    }

    public <K, V> TestConsumer<K, V> consumer(String topic) {
        return consumer(topic,
                properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).toString(),
                properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).toString());
    }

    public <K, V> TestConsumer<K, V> consumer(String topic, Class<? extends Deserializer<K>> keyDeserializerClass, Class<? extends Deserializer<V>> valueDeserializerClass) {
        return consumer(topic, keyDeserializerClass.getName(), valueDeserializerClass.getName());
    }

    private <K, V> TestConsumer<K, V> consumer(String topic, String keyDeserializerClassName, String valueDeserializerClassName) {
        this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);

        return new TestConsumer<>(topic, new KafkaConsumer<>(this.properties));
    }

    public <K, V> TestProducer<K, V> producer(String topic) {
        return producer(topic,
                properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).toString(),
                properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).toString());
    }

    public <K, V> TestProducer<K, V> producer(String topic, Class<? extends Serializer<K>> keySerializerClass, Class<? extends Serializer<V>> valueSerializerClass) {
        return producer(topic, keySerializerClass.getName(), valueSerializerClass.getName());
    }

    private <K, V> TestProducer<K, V> producer(String topic, String keySerializerClassName, String valueSerializerClassName) {
        this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);

        return new TestProducer<>(topic, new KafkaProducer<>(this.properties));
    }

    private void setConsumerProperties() {
        this.properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testConsumersGroup-" + randomAlphabetic(8));
        this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + randomAlphabetic(8));
        this.properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName());
    }

    private void setProducerProperties() {
        this.properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + randomAlphabetic(8));
        this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    }
}
