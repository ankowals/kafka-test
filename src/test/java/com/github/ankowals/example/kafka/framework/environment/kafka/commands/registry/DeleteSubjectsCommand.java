package com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.function.Failable;

public class DeleteSubjectsCommand implements SchemaRegistryClientCommand {

  private final List<String> subjects;

  DeleteSubjectsCommand(String... subjects) {
    this.subjects = this.map(subjects);
  }

  @Override
  public void using(SchemaRegistryClient schemaRegistryClient) throws Exception {
    List<String> actualSubjects = SchemaRegistrySubjects.get().using(schemaRegistryClient);

    Failable.stream(this.subjects)
        .filter(actualSubjects::contains)
        .forEach(schemaRegistryClient::deleteSubject);
  }

  private List<String> map(String... subjects) {
    return Arrays.stream(subjects)
        .map(subject -> subject.endsWith("-value") ? subject : String.format("%s-value", subject))
        .toList();
  }
}
