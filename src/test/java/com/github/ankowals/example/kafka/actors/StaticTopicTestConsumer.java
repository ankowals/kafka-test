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

    private final BlockingQueue<String> records = new LinkedBlockingDeque<>();

    @Topic("testTopic")
    public void consume(String body) {
        records.add(body);
    }

    public String getRecord() throws InterruptedException {
        String record = records.poll(2, TimeUnit.SECONDS);
        records.clear();

        return record;
    }

    public BlockingQueue<String> getRecords() {
        BlockingQueue<String> copy = new LinkedBlockingDeque<>(records);
        records.clear();

        return copy;
    }
}
