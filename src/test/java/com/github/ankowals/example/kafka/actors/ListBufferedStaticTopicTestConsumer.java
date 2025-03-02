package com.github.ankowals.example.kafka.actors;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Singleton
@KafkaListener(
    groupId = "list-buffered-topic-test-consumers",
    clientId = "list-buffered-topic-test-consumer")
public class ListBufferedStaticTopicTestConsumer {

  private final List<String> buffer = new CopyOnWriteArrayList<>();

  @Topic("test-topic")
  public void consume(String aRecord) {
    this.buffer.add(aRecord);
  }

  public List<String> getRecords() {
    return List.copyOf(this.buffer);
  }

  public void clear() {
    this.buffer.clear();
  }
}
