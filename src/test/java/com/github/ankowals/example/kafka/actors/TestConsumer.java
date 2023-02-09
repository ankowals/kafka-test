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

    TestConsumer(KafkaConsumer<K, V> kafkaConsumer) {
        this.actual = new CopyOnWriteArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
        this.service = Executors.newSingleThreadExecutor();
    }

    public TestConsumer<K, V> subscribe(String topic) {
        kafkaConsumer.subscribe(List.of(topic));

        this.consumingTask = service.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<K, V> rec : records) {
                    actual.add(rec.value());
                }
            }
        });

        return this;
    }

    public void close() throws InterruptedException {
        consumingTask.cancel(true);
        service.awaitTermination(1, SECONDS);
        kafkaConsumer.close();
    }

    public List<V> consumeAndClose() throws InterruptedException {
        try {
            return consume();
        } finally {
            close();
        }
    }

    public List<V> consume() {
        List<V> copy = getActual();
        actual.clear();

        return copy;
    }

    public V consumeLatest() {
        return actual.get(actual.size() - 1);
    }

    public List<V> consumeUntil(Predicate<List<V>> predicate) throws InterruptedException {
        try {
            Callable<List<V>> supplier = this::getActual;
            return await().until(supplier, predicate);
        } finally {
            close();
        }
    }

    private List<V> getActual() {
        return List.copyOf(actual);
    }
}
