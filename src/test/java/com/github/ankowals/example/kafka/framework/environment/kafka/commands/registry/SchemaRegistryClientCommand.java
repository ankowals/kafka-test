package com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

@FunctionalInterface
public interface SchemaRegistryClientCommand {
  void using(SchemaRegistryClient schemaRegistryClient) throws Exception;
}
