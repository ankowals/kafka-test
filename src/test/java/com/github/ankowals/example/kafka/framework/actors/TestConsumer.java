package com.github.ankowals.example.kafka.framework.actors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;

public class TestConsumer<K, V> {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaConsumer<K, V> kafkaConsumer;
    private final List<V> buffer;

    private ExecutorService service;
    private Future<?> consumingTask;

    public TestConsumer(String topic, KafkaConsumer<K, V> kafkaConsumer) {
        this.buffer = new CopyOnWriteArrayList<>();
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaConsumer.subscribe(List.of(validateTopic(topic)));
    }

    public List<V> consume() {
        return consume(DEFAULT_TIMEOUT);
    }

    public List<V> consume(Duration timeout) {
        try {
            poll(timeout);

            List<V> copy = getBufferCopy();
            buffer.clear();

            return copy;
        } finally {
            kafkaConsumer.close();
        }
    }

    public List<V> consumeUntil(Predicate<List<V>> predicate) throws InterruptedException {
        return consumeUntil(predicate, DEFAULT_TIMEOUT);
    }

    public List<V> consumeUntil(Predicate<List<V>> predicate, Duration timeout) throws InterruptedException {
        try {
            Callable<List<V>> supplier = this::getBufferCopy;
            startConsuming();

            return await().atMost(timeout).until(supplier, predicate);
        } finally {
            buffer.clear();
            service.awaitTermination(300, MILLISECONDS);
            consumingTask.cancel(true);
        }
    }

    public V consumeUntilMatch(Predicate<V> predicate) throws InterruptedException {
        return consumeUntilMatch(predicate, DEFAULT_TIMEOUT);
    }

    public V consumeUntilMatch(Predicate<V> predicate, Duration timeout) throws InterruptedException {
        return getMatching(consumeUntil(list -> list.stream().anyMatch(predicate), timeout), predicate);
    }

    private static <T> T getMatching(List<T> list, Predicate<T> predicate) {
        return list != null && !list.isEmpty()
                ? list.stream().filter(predicate).findAny().orElse(null)
                : null;
    }

    private void startConsuming() {
        this.service = Executors.newSingleThreadExecutor();
        this.consumingTask = service.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                poll(Duration.ofMillis(100));
            }
        });
    }

    private void poll(Duration duration) {
        ConsumerRecords<K, V> records = kafkaConsumer.poll(duration);
        for (ConsumerRecord<K, V> rec : records) {
            buffer.add(rec.value());
            kafkaConsumer.commitSync();
        }
    }

    private List<V> getBufferCopy() {
        return List.copyOf(buffer);
    }

    private String validateTopic(String name) {
        if (isNullOrEmpty(name))
            throw new IllegalArgumentException("Topic name can't be null or empty!");

        return name;
    }

    private boolean isNullOrEmpty(String s) {
        return s == null || s.equals("");
    }
}
