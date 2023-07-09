package com.github.ankowals.example.kafka.actors;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Singleton
@KafkaListener(groupId = "list-buffered-static-topic-test-consumers", clientId = "list-buffered-static-topic-test-consumer")
public class ListBufferedStaticTopicTestConsumer {

    private final List<String> buffer = new CopyOnWriteArrayList<>();

    @Topic("test-topic")
    public void consume(String record) {
        this.buffer.add(record);
    }

    public List<String> getRecords() {
        return List.copyOf(this.buffer);
    }

    public void clearBuffer() {
        this.buffer.clear();
    }
}
