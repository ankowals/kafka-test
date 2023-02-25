package com.github.ankowals.example.kafka.environment;

import com.github.ankowals.example.kafka.framework.environment.Kafka;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.HashMap;
import java.util.Map;

import static org.testcontainers.containers.KafkaContainer.KAFKA_PORT;

public interface UsesKafka {

    Kafka KAFKA_INSTANCE = Kafka.start();

    default AdminClient getAdminClient() { return KAFKA_INSTANCE.getAdminClient(); }

    default Map<String, String> getKafkaProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("kafka.bootstrap.servers", KAFKA_INSTANCE.getContainer().getHost() + ":" + KAFKA_INSTANCE.getContainer().getMappedPort(KAFKA_PORT));
        properties.put("kafka.schema.registry.url", "mock://schema-registry");

        return properties;
    }
}
