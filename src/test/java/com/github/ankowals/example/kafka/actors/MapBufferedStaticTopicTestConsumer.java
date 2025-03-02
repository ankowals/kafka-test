package com.github.ankowals.example.kafka.actors;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Singleton
@KafkaListener(
    groupId = "map-buffered-topic-test-consumers",
    clientId = "map-buffered-topic-test-consumer")
public class MapBufferedStaticTopicTestConsumer<T> {

  private final Map<String, T> buffer = new ConcurrentHashMap<>();

  @Topic("test-topic")
  public void consume(ConsumerRecord<String, T> consumerRecord) {
    if (consumerRecord.key() == null) {
      System.out.printf(
          "No key for record at offset %s, topic %s, partition %s%n",
          consumerRecord.offset(), consumerRecord.topic(), consumerRecord.partition());
    } else {
      this.buffer.put(consumerRecord.key(), consumerRecord.value());
    }
  }

  public Map<String, T> getRecords() {
    return this.buffer.entrySet().stream()
        .map(Map.Entry::copyOf)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public void clear() {
    this.buffer.clear();
  }
}
