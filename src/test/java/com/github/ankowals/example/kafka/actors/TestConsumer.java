package com.github.ankowals.example.kafka.actors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public class TestConsumer<K, V> {

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<V> actual;
    private final ExecutorService service;
    private Future<?> consumingTask;
    private String topic;

    TestConsumer(KafkaConsumer<K, V> kafkaConsumer) {
        this.actual = new CopyOnWriteArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
        this.service = Executors.newSingleThreadExecutor();
    }

    public TestConsumer<K, V> subscribe(String topic) {
        if (isNullOrEmpty(topic)) {
            throw new IllegalArgumentException("Topic can't be null or empty!");
        }

        kafkaConsumer.subscribe(List.of(topic));

        this.consumingTask = service.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<K, V> rec : records) {
                    actual.add(rec.value());
                }
            }
        });

        this.topic = topic;

        return this;
    }

    public void close() throws InterruptedException {
        consumingTask.cancel(true);
        service.awaitTermination(1, SECONDS);
        kafkaConsumer.close();
    }

    public List<V> consume(String topic) throws InterruptedException {
        try {
            if (isNullOrEmpty(topic)) {
                throw new IllegalArgumentException("Topic can't be null or empty!");
            }
            if (!topic.equals(this.topic)) {
                subscribe(topic);
            }

            return consume();
        } finally {
            close();
        }
    }

    public List<V> consume() {
        if (this.topic == null) {
            throw new IllegalStateException("Topic subscription not found! Call subscribe() first!");
        }

        List<V> copy = getActual();
        actual.clear();

        return copy;
    }

    public V consumeLatest() {
        if (this.topic == null) {
            throw new IllegalStateException("Topic subscription not found! Call subscribe() first!");
        }

        return actual.get(actual.size() - 1);
    }

    public V consumeLatest(String topic) throws InterruptedException {
        try {
            if (isNullOrEmpty(topic)) {
                throw new IllegalArgumentException("Topic can't be null or empty!");
            }
            if (!topic.equals(this.topic)) {
                subscribe(topic);
            }

            return consumeLatest();
        } finally {
            close();
        }
    }

    public List<V> consumeUntil(String topic, Predicate<List<V>> predicate) throws InterruptedException {
        try {
            if (isNullOrEmpty(topic)) {
                throw new IllegalArgumentException("Topic can't be null or empty!");
            }
            if (!topic.equals(this.topic)) {
                subscribe(topic);
            }

            Callable<List<V>> supplier = this::getActual;
            return await().until(supplier, predicate);
        } finally {
            close();
        }
    }

    private List<V> getActual() {
        return List.copyOf(actual);
    }

    private boolean isNullOrEmpty(String s) {
        return s == null || s.equals("");
    }
}
