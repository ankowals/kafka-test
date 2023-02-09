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

public class CustomTestConsumer<K, V> {

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<V> actual;
    private final ExecutorService service;
    private Future<?> consumingTask;

    CustomTestConsumer(KafkaConsumer<K, V> kafkaConsumer) {
        this.actual = new CopyOnWriteArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
        this.service = Executors.newSingleThreadExecutor();
    }

    public void subscribe(String topic) {
        kafkaConsumer.subscribe(List.of(topic));

        this.consumingTask = service.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<K, V> rec : records) {
                    actual.add(rec.value());
                }
            }
        });
    }

    public void cancel() throws InterruptedException {
        consumingTask.cancel(true);
        service.awaitTermination(1, SECONDS);
        kafkaConsumer.close();
    }

    public List<V> getRecordsAndCancel() throws InterruptedException {
        try {
            List<V> copy = getActual();
            actual.clear();

            return copy;
        } finally {
            cancel();
        }
    }

    public List<V> getRecords() {
        List<V> copy = getActual();
        actual.clear();

        return copy;
    }

    public V getRecord() {
        return actual.get(actual.size() - 1);
    }

    public List<V> consumeUntil(Predicate<List<V>> predicate) throws InterruptedException {
        try {
            Callable<List<V>> supplier = this::getActual;
            return await().until(supplier, predicate);
        } finally {
            cancel();
        }
    }

    private List<V> getActual() {
        return List.copyOf(actual);
    }
}
