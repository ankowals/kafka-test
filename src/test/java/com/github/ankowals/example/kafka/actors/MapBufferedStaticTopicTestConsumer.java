package com.github.ankowals.example.kafka.actors;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Singleton
@KafkaListener(groupId = "map-buffered-topic-test-consumers", clientId = "map-buffered-topic-test-consumer")
public class MapBufferedStaticTopicTestConsumer<T> {

    private final Map<String, T> buffer = new ConcurrentHashMap<>();

    @Topic("test-topic")
    public void consume(ConsumerRecord<String, T> record) {
        if (record.key() == null) {
            System.out.printf("No key for record at offset %s, topic %s, partition %s%n",
                    record.offset(),
                    record.topic(),
                    record.partition());
        } else {
            this.buffer.put(record.key(), record.value());
        }
    }

    public Map<String, T> getRecords() {
        return this.buffer.entrySet()
                .stream()
                .map(Map.Entry::copyOf)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void clear() {
        this.buffer.clear();
    }
}
