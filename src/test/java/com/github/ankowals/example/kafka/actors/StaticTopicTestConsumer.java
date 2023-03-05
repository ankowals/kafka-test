package com.github.ankowals.example.kafka.actors;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Singleton;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Singleton
@KafkaListener(groupId = "testConsumers", clientId = "testTopic-consumer")
public class StaticTopicTestConsumer {

    private final BlockingQueue<String> buffer = new LinkedBlockingDeque<>();

    @Topic("testTopic")
    public void consume(String record) {
        buffer.add(record);
    }

    public String getRecord() throws InterruptedException {
        String record = buffer.poll(2, TimeUnit.SECONDS);
        buffer.clear();

        return record;
    }

    public BlockingQueue<String> getBuffer() {
        BlockingQueue<String> copy = new LinkedBlockingDeque<>(buffer);
        buffer.clear();

        return copy;
    }
}
