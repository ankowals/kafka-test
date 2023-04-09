package com.github.ankowals.example.kafka.actors;

import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.Creator;
import jakarta.inject.Singleton;
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

@Singleton
public class TestActorFactory {

    private final Properties properties;

    public TestActorFactory(@Property(name = "kafka.bootstrap.servers") String bootstrapServer) {
        this(bootstrapServer, "");
    }

    @Creator
    public TestActorFactory(@Property(name = "kafka.bootstrap.servers") String bootstrapServer,
                            @Property(name = "kafka.schema.registry.url") String schemaRegistryUrl) {
        this.properties = new Properties();
        this.properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        this.properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        this.setConsumerProperties();
        this.setProducerProperties();
    }

    public TestActorFactory(Properties properties) {
        this.properties = properties;
    }

    public <K, V> TestConsumer<K, V> consumer(String topic) {
        return consumer(topic,
                this.properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).toString(),
                this.properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).toString());
    }

    public <K, V> TestConsumer<K, V> consumer(String topic,
                                              Class<? extends Deserializer<K>> keyDeserializerClass,
                                              Class<? extends Deserializer<V>> valueDeserializerClass) {
        return this.consumer(topic, keyDeserializerClass.getName(), valueDeserializerClass.getName());
    }

    private <K, V> TestConsumer<K, V> consumer(String topic,
                                               String keyDeserializerClassName,
                                               String valueDeserializerClassName) {
        this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);

        this.properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test-consumer-" + randomAlphabetic(11));
        this.properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-" + randomAlphabetic(11));

        return new TestConsumer<>(topic, new KafkaConsumer<>(this.properties));
    }

    public <K, V> TestProducer<K, V> producer(String topic) {
        return producer(topic,
                this.properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).toString(),
                this.properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).toString());
    }

    public <K, V> TestProducer<K, V> producer(String topic,
                                              Class<? extends Serializer<K>> keySerializerClass,
                                              Class<? extends Serializer<V>> valueSerializerClass) {
        return this.producer(topic, keySerializerClass.getName(), valueSerializerClass.getName());
    }

    private <K, V> TestProducer<K, V> producer(String topic, String keySerializerClassName, String valueSerializerClassName) {
        this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);

        this.properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer-" + randomAlphabetic(11));

        return new TestProducer<>(topic, new KafkaProducer<>(this.properties));
    }

    private void setConsumerProperties() {
        this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
        this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GenericAvroDeserializer.class.getName());
    }

    private void setProducerProperties() {
        this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        this.properties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);
        this.properties.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        this.properties.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
    }
}
