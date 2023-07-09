package com.github.ankowals.example.kafka.environment;

import com.github.ankowals.example.kafka.framework.environment.kafka.Kafka;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin.KafkaTopics;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.HashMap;
import java.util.Map;

public interface UsesKafka {

    Kafka KAFKA_INSTANCE = Kafka.start(KafkaTopics.create("word-input", "word-output"));

    default AdminClient getAdminClient() { return KAFKA_INSTANCE.getAdminClient(); }

    default SchemaRegistryClient getSchemaRegistryClient() {
        return KAFKA_INSTANCE.getSchemaRegistryClient();
    }

    default Map<String, String> getKafkaProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kafka.bootstrap.servers", KAFKA_INSTANCE.getContainer().getBootstrapServers());
        properties.put("kafka.schema.registry.url", KAFKA_INSTANCE.getContainer().getSchemaRegistryUrl());
        properties.put("kafka.streams.default.internal.leave.group.on.close", "true"); //to avoid re-balancing because of frequent app restarts in tests

        return properties;
    }
}
