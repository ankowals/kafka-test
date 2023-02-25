package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.framework.environment.kafka.SchemaReader;
import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.actors.TestActorFactory;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import com.github.ankowals.example.kafka.TestBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.github.ankowals.example.kafka.framework.environment.kafka.TestTopicCreateCommand.createTopics;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class DynamicTopicTest extends TestBase {

    private TestActorFactory actorFactory;
    private String topic;

    @BeforeEach
    void setup() throws Exception {
        this.topic = randomAlphabetic(11);
        this.actorFactory = new TestActorFactory(
                getProperties().get("kafka.bootstrap.servers"),
                getProperties().get("kafka.schema.registry.url"));

        createTopics(topic).run(getAdminClient());
    }

    @Test
    public void shouldConsumeRecords() {
        Integer randomNumber = nextInt();

        TestProducer<String, Integer> producer = actorFactory.producer(topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = actorFactory.consumer(topic, StringDeserializer.class, IntegerDeserializer.class);

        producer.produce(randomNumber);
        List<Integer> messages = consumer.consume();

        assertThat(messages.size()).isEqualTo(1);
        assertThat(messages.get(messages.size() - 1)).isEqualTo(randomNumber);
    }

    @Test
    public void shouldConsumeRecordsUntilConditionIsFulfilled() throws InterruptedException {
        TestProducer<String, String> producer = actorFactory.producer(topic, StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = actorFactory.consumer(topic, StringDeserializer.class, StringDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).forEach(i -> producer.send("terefere-" + i));
            producer.close();
        });

        List<String> messages = consumer.consumeUntil(numberOfRecordsIs(999));

        assertThat(messages.size()).isEqualTo(999);
        assertThat(messages).contains("terefere-999");
    }

    @Test
    public void shouldConsumeLatestRecord() throws InterruptedException {
        TestProducer<String, Integer> producer = actorFactory.producer(topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = actorFactory.consumer(topic, StringDeserializer.class, IntegerDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).forEach(producer::send);
            producer.close();
        });

        Integer message = consumer.consumeUntilMatch(number -> number == 999);

        assertThat(message).isEqualTo(999);
    }

    @Test
    public void shouldPollUntilRecordsFound() throws InterruptedException {
        TestProducer<String, Integer> producer = actorFactory.producer(topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = actorFactory.consumer(topic, StringDeserializer.class, IntegerDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).forEach(producer::send);
            producer.close();
        });

        List<Integer> messages = consumer.consumeUntil(anyRecordFound());

        assertThat(messages.size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void shouldStartPollingFirst() {
        TestProducer<String, Integer> producer = actorFactory.producer(topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = actorFactory.consumer(topic, StringDeserializer.class, IntegerDeserializer.class);

        AtomicReference<List<Integer>> messages = new AtomicReference<>();

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                messages.set(consumer.consumeUntil(list -> list.contains(459)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).forEach(producer::send);
            producer.close();
        });

        await().until(() -> messages.get() != null);

        assertThat(messages.get().size()).isGreaterThanOrEqualTo(459);
    }

    @Test
    public void shouldConsumeGenericRecord() throws IOException, InterruptedException {
        TestProducer<Bytes, Object> producer = actorFactory.producer(topic);
        TestConsumer<Bytes, GenericRecord> consumer = actorFactory.consumer(topic);

        Schema schema = new SchemaReader().read("user.avro");

        String name = randomAlphabetic(11);

        GenericData.Record record = new GenericRecordBuilder(schema)
                .set("name", name)
                .set("favorite_number", 7)
                .set("favorite_color", "blue")
                .build();

        producer.produce(record);
        GenericRecord message = consumer.consumeUntilMatch(recordNameEquals(name));

        assertThat(message.toString())
                .isEqualTo("{\"name\": \"" + name + "\", \"favorite_number\": 7, \"favorite_color\": \"blue\"}");
    }

    private Predicate<List<Integer>> anyRecordFound() {
        return list -> list.size() > 0;
    }

    private Predicate<List<String>> numberOfRecordsIs(int number) {
        return list -> list.size() == number;
    }

    private Predicate<GenericRecord> recordNameEquals(String name) {
        return genericRecord -> new JSONObject(genericRecord.toString()).getString("name").equals(name);
    }
}
