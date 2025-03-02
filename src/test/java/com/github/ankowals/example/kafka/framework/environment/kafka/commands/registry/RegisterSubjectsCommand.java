package com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class RegisterSubjectsCommand implements SchemaRegistryClientCommand {

  private final String subject;
  private final AvroSchema avroSchema;

  RegisterSubjectsCommand(String subject, AvroSchema avroSchema) {
    this.subject = this.map(subject);
    this.avroSchema = avroSchema;
  }

  @Override
  public void using(SchemaRegistryClient schemaRegistryClient) throws Exception {
    schemaRegistryClient.register(this.subject, this.avroSchema);
  }

  private String map(String subject) {
    return subject.endsWith("-value") ? subject : String.format("%s-value", subject);
  }
}
