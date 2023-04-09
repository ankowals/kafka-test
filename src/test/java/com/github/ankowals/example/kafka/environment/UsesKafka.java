package com.github.ankowals.example.kafka.environment;

import com.github.ankowals.example.kafka.framework.environment.kafka.Kafka;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.KafkaCreate;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.HashMap;
import java.util.Map;

public interface UsesKafka {

    Kafka KAFKA_INSTANCE = Kafka.start(KafkaCreate.topics("word-input", "word-output"));

    default AdminClient getAdminClient() { return KAFKA_INSTANCE.getAdminClient(); }

    default Map<String, String> getKafkaProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kafka.bootstrap.servers", KAFKA_INSTANCE.getContainer().getBootstrapServers());
        properties.put("kafka.schema.registry.url", "mock://schema-registry");

        return properties;
    }
}
