package com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;

import java.util.List;

public class SchemaRegistrySubjects {

    public static RegisterSubjectsCommand register(String subject, AvroSchema avroSchema) {
        return new RegisterSubjectsCommand(subject, avroSchema);
    }

    public static SchemaRegistryClientQuery<List<String>> get() {
        return schemaRegistryClient -> schemaRegistryClient.getAllSubjects()
                .stream()
                .toList();
    }
}
