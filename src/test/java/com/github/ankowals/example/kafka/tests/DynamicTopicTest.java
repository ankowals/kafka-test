package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.framework.environment.kafka.SchemaReader;
import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.actors.TestActorFactory;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import com.github.ankowals.example.kafka.IntegrationTestBase;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.KafkaCreate;
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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class DynamicTopicTest extends IntegrationTestBase {

    private TestActorFactory actorFactory;
    private String topic;

    @BeforeEach
    void setup() throws Exception {
        this.topic = randomAlphabetic(11);
        this.actorFactory = new TestActorFactory(
                this.getProperties().get("kafka.bootstrap.servers"),
                this.getProperties().get("kafka.schema.registry.url"));

        KafkaCreate.topics(this.topic).run(this.getAdminClient());
    }

    @Test
    public void shouldConsumeRecords() {
        Integer record = nextInt();

        TestProducer<String, Integer> producer = this.actorFactory.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.actorFactory.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        producer.produce(record);
        List<Integer> records = consumer.consume();

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(records.size() - 1)).isEqualTo(record);
    }

    @Test
    public void shouldConsumeRecordsUntilConditionIsFulfilled() throws InterruptedException {
        TestProducer<String, String> producer = this.actorFactory.producer(this.topic, StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = this.actorFactory.consumer(this.topic, StringDeserializer.class, StringDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).forEach(i -> producer.send("terefere-" + i));
            producer.close();
        });

        List<String> records = consumer.consumeUntil(this.numberOfRecordsIs(999));

        assertThat(records.size()).isEqualTo(999);
        assertThat(records).contains("terefere-999");
    }

    @Test
    public void shouldConsumeLatestRecord() throws InterruptedException {
        TestProducer<String, Integer> producer = this.actorFactory.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.actorFactory.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
        });

        Integer record = consumer.consumeUntilMatch(number -> number == 999);

        assertThat(record).isEqualTo(999);
    }

    @Test
    public void shouldPollUntilRecordsFound() throws InterruptedException {
        TestProducer<String, Integer> producer = this.actorFactory.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.actorFactory.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
        });

        List<Integer> records = consumer.consumeUntil(this.anyRecordFound());

        assertThat(records.size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void shouldStartPollingFirst() {
        TestProducer<String, Integer> producer = this.actorFactory.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.actorFactory.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        AtomicReference<List<Integer>> records = new AtomicReference<>();

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                records.set(consumer.consumeUntil(list -> list.contains(459)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
        });

        await().until(() -> records.get() != null);

        assertThat(records.get().size()).isGreaterThanOrEqualTo(459);
    }

    @Test
    public void shouldPollUntilExpectedElementsFound() throws InterruptedException {
        TestProducer<String, String> producer = this.actorFactory.producer(this.topic, StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = this.actorFactory.consumer(this.topic, StringDeserializer.class, StringDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(i -> producer.send("terefere-" + i));
            producer.close();
        });

        List<String> expectedRecords = List.of("terefere-3", "terefere-459", "terefere-812");
        List<String> records = consumer.consumeUntil(this.containsAll(expectedRecords));

        assertThat(records).doesNotHaveDuplicates();
    }

    @Test
    public void shouldConsumeGenericRecord() throws IOException, InterruptedException {
        TestProducer<Bytes, Object> producer = this.actorFactory.producer(this.topic);
        TestConsumer<Bytes, GenericRecord> consumer = this.actorFactory.consumer(this.topic);

        Schema schema = new SchemaReader().read("user.avro");

        String name = randomAlphabetic(11);

        GenericData.Record record = new GenericRecordBuilder(schema)
                .set("name", name)
                .set("favorite_number", 7)
                .set("favorite_color", "blue")
                .build();

        producer.produce(record);
        GenericRecord actualRecord = consumer.consumeUntilMatch(this.recordNameEquals(name));

        assertThat(actualRecord.toString())
                .isEqualTo("{\"name\": \"" + name + "\", " +
                        "\"favorite_number\": 7, " +
                        "\"favorite_color\": \"blue\"}");
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

    private Predicate<List<String>> containsAll(List<String> sublist) {
        return list-> list.containsAll(sublist);
    }
}
