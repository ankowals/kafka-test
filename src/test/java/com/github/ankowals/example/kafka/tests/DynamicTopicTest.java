package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.framework.actors.TestActors;
import com.github.ankowals.example.kafka.framework.environment.kafka.SchemaLoader;
import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import com.github.ankowals.example.kafka.IntegrationTestBase;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin.KafkaTopics;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry.SchemaRegistrySubjects;
import com.github.ankowals.example.kafka.predicates.Predicates;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public class DynamicTopicTest extends IntegrationTestBase {

    @Inject
    private TestActors testActors;
    private String topic;

    @BeforeEach
    void createTopic() throws Exception {
        this.topic = RandomStringUtils.randomAlphabetic(11);
        KafkaTopics.create(this.topic).using(this.getAdminClient());
    }

    @Test
    public void shouldConsumeRecords() {
        Integer record = nextInt();

        TestProducer<String, Integer> producer = this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        producer.produce(record);
        List<Integer> actual = consumer.consume();

        Assertions.assertThat(actual.size()).isEqualTo(1);
        Assertions.assertThat(actual.get(actual.size() - 1)).isEqualTo(record);
    }

    @Test
    public void shouldConsumeRecordsUntilConditionIsFulfilled() throws InterruptedException {
        TestProducer<String, String> producer = this.testActors.producer(this.topic, StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = this.testActors.consumer(this.topic, StringDeserializer.class, StringDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).forEach(i -> producer.send("terefere-" + i));
            producer.close();
        });

        List<String> actual = consumer.consumeUntil(Predicates.numberOfRecordsIs(999));

        Assertions.assertThat(actual.size()).isEqualTo(999);
        Assertions.assertThat(actual).contains("terefere-999");
    }

    @Test
    public void shouldConsumeLatestRecord() throws InterruptedException {
        TestProducer<String, Integer> producer = this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
        });

        Integer actual = consumer.consumeUntilMatch(number -> number == 876);

        Assertions.assertThat(actual).isEqualTo(876);
    }

    @Test
    public void shouldPollUntilRecordsFound() throws InterruptedException {
        TestProducer<String, Integer> producer = this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
        });

        List<Integer> actual = consumer.consumeUntil(Predicates.anyRecordFound());

        Assertions.assertThat(actual.size()).isGreaterThanOrEqualTo(1);
    }

    @Test
    public void shouldStartPollingFirst() {
        TestProducer<String, Integer> producer = this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

        AtomicReference<List<Integer>> actual = new AtomicReference<>();

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                actual.set(consumer.consumeUntil(list -> list.contains(459)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
        });

        Awaitility.await().until(() -> actual.get() != null);

        Assertions.assertThat(actual).hasValueMatching(l -> l.contains(459));
    }

    @Test
    public void shouldPollUntilExpectedElementsFound() throws InterruptedException {
        TestProducer<String, String> producer = this.testActors.producer(this.topic, StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = this.testActors.consumer(this.topic, StringDeserializer.class, StringDeserializer.class);

        List<String> noise = IntStream.range(1, 1000)
                                .mapToObj(i -> RandomStringUtils.randomAlphabetic(8))
                                .toList();

        List<String> expected = List.of(
                RandomStringUtils.randomAlphabetic(8),
                RandomStringUtils.randomAlphabetic(8),
                RandomStringUtils.randomAlphabetic(8));

        List<String> input = this.mergeAndShuffle(noise, expected);

        Executors.newSingleThreadExecutor().submit(() -> {
            input.stream().parallel().forEach(producer::send);
            producer.close();
        });

        List<String> actual = consumer.consumeUntil(Predicates.containsAll(expected));

        Assertions.assertThat(actual).doesNotHaveDuplicates();
    }

    @Test
    public void shouldConsumeGenericRecord() throws Exception {
        Schema schema = new SchemaLoader().load("user.avro");
        SchemaRegistrySubjects.register(this.topic, new AvroSchema(schema)).using(this.getSchemaRegistryClient());

        TestProducer<Bytes, Object> producer = this.testActors.producer(this.topic);
        TestConsumer<Bytes, GenericRecord> consumer = this.testActors.consumer(this.topic);

        String name = RandomStringUtils.randomAlphabetic(11);

        GenericData.Record record = new GenericRecordBuilder(schema)
                .set("name", name)
                .set("favorite_number", 7)
                .set("favorite_color", "blue")
                .build();

        producer.produce(record);
        GenericRecord actual = consumer.consumeUntilMatch(Predicates.recordNameEquals(name));

        assertThat(actual.toString())
                .isEqualTo("{\"name\": \"" + name + "\", " +
                        "\"favorite_number\": 7, " +
                        "\"favorite_color\": \"blue\"}");
    }

    private List<String> mergeAndShuffle(List<String> list1, List<String> list2) {
        List<String> tmp = new ArrayList<>();
        tmp.addAll(list1);
        tmp.addAll(list2);

        Collections.shuffle(tmp);

        return tmp;
    }
}