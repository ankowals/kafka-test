package com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

@FunctionalInterface
public interface SchemaRegistryClientQuery<T> {
    T using(SchemaRegistryClient schemaRegistryClient) throws Exception;
}
