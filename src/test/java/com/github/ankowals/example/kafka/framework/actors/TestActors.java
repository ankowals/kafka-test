package com.github.ankowals.example.kafka.framework.actors;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import org.apache.commons.lang3.RandomStringUtils;
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

public class TestActors {

    private final String bootstrapServer;
    private final String schemaRegistryUrl;

    public TestActors(String bootstrapServer, String schemaRegistryUrl) {
        this.bootstrapServer = bootstrapServer;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public TestActors(String bootstrapServer) {
        this(bootstrapServer, "");
    }

    public TestActors(Properties properties) {
        this.bootstrapServer = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "");
        this.schemaRegistryUrl = properties.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "");
    }

    public <K, V> TestConsumer<K, V> consumer(String topic, Properties properties) {
        return new TestConsumer<>(topic, new KafkaConsumer<>(properties));
    }

    public <K, V> TestConsumer<K, V> consumer(String topic) {
        return this.consumer(topic, this.createConsumerProperties());
    }

    public <K, V> TestConsumer<K, V> consumer(String topic,
                                              Class<? extends Deserializer<K>> keyDeserializerClass,
                                              Class<? extends Deserializer<V>> valueDeserializerClass) {
        return this.consumer(topic, this.createConsumerProperties(keyDeserializerClass.getName(), valueDeserializerClass.getName()));
    }

    public <K, V> TestProducer<K, V> producer(String topic, Properties properties) {
        return new TestProducer<>(topic, new KafkaProducer<>(properties));
    }

    public <K, V> TestProducer<K, V> producer(String topic) {
        return this.producer(topic, this.createProducerProperties());
    }

    public <K, V> TestProducer<K, V> producer(String topic,
                                              Class<? extends Serializer<K>> keySerializerClass,
                                              Class<? extends Serializer<V>> valueSerializerClass) {
        return this.producer(topic, this.createProducerProperties(keySerializerClass.getName(), valueSerializerClass.getName()));
    }

    private Properties createConsumerProperties(String keyDeserializerClassName, String valueDeserializerClassName) {
        Properties properties = this.createConsumerProperties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);

        return properties;
    }

    private Properties createProducerProperties(String keySerializer, String valueSerializer) {
        Properties properties = this.createProducerProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        return properties;
    }

    private Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + RandomStringUtils.randomAlphabetic(11));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + RandomStringUtils.randomAlphabetic(11));
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName());

        return properties;
    }

    private Properties createProducerProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + RandomStringUtils.randomAlphabetic(11));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        properties.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        properties.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);

        return properties;
    }
}
