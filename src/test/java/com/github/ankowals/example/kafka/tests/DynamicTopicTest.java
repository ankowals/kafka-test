package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.IntegrationTestBase;
import com.github.ankowals.example.kafka.framework.actors.TestActors;
import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import com.github.ankowals.example.kafka.framework.environment.kafka.Schemas;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin.KafkaTopics;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.registry.SchemaRegistrySubjects;
import com.github.ankowals.example.kafka.predicates.RecordPredicates;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@MicronautTest
class DynamicTopicTest extends IntegrationTestBase {

  @Inject TestActors testActors;

  String topic;

  @BeforeEach
  void createTopic() throws Exception {
    this.topic = RandomStringUtils.insecure().nextAlphabetic(11);
    KafkaTopics.create(this.topic).using(this.getAdminClient());
  }

  @Test
  void shouldConsumeRecords() {
    Integer nextInt = RandomUtils.insecure().randomInt();

    TestProducer<String, Integer> producer =
        this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
    TestConsumer<String, Integer> consumer =
        this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

    producer.produce(nextInt);
    List<Integer> actual = consumer.consume();

    Assertions.assertThat(actual).hasSize(1);
    Assertions.assertThat(actual.getFirst()).isEqualTo(nextInt);
  }

  @Test
  void shouldConsumeRecordsUntilConditionIsFulfilled() throws InterruptedException {
    TestProducer<String, String> producer =
        this.testActors.producer(this.topic, StringSerializer.class, StringSerializer.class);
    TestConsumer<String, String> consumer =
        this.testActors.consumer(this.topic, StringDeserializer.class, StringDeserializer.class);

    try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
      executorService.submit(
          () -> {
            (IntStream.range(1, 1000)).forEach(i -> producer.send("terefere-" + i));
            producer.close();
          });
    }

    List<String> actual = consumer.consumeUntil(RecordPredicates.sizeIs(999));

    Assertions.assertThat(actual).hasSize(999).contains("terefere-999");
  }

  @Test
  void shouldConsumeLatestRecord() throws InterruptedException {
    TestProducer<String, Integer> producer =
        this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
    TestConsumer<String, Integer> consumer =
        this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

    try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
      executorService.submit(
          () -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
          });
    }

    Integer actual = consumer.consumeUntilMatch(number -> number == 876);

    Assertions.assertThat(actual).isEqualTo(876);
  }

  @Test
  void shouldPollUntilRecordsFound() throws InterruptedException {
    TestProducer<String, Integer> producer =
        this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
    TestConsumer<String, Integer> consumer =
        this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

    try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
      executorService.submit(
          () -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
          });
    }

    List<Integer> actual = consumer.consumeUntil(RecordPredicates.anyFound());

    Assertions.assertThat(actual).isNotEmpty();
  }

  @Test
  void shouldStartPollingFirst() {
    TestProducer<String, Integer> producer =
        this.testActors.producer(this.topic, StringSerializer.class, IntegerSerializer.class);
    TestConsumer<String, Integer> consumer =
        this.testActors.consumer(this.topic, StringDeserializer.class, IntegerDeserializer.class);

    AtomicReference<List<Integer>> actual = new AtomicReference<>();

    try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
      executorService.submit(
          () -> {
            try {
              actual.set(consumer.consumeUntil(list -> list.contains(459)));
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    }

    try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
      executorService.submit(
          () -> {
            (IntStream.range(1, 1000)).parallel().forEach(producer::send);
            producer.close();
          });
    }

    Awaitility.await().until(() -> actual.get() != null);

    Assertions.assertThat(actual).hasValueMatching(l -> l.contains(459));
  }

  @Test
  void shouldPollUntilExpectedElementsFound() throws InterruptedException {
    TestProducer<String, String> producer =
        this.testActors.producer(this.topic, StringSerializer.class, StringSerializer.class);
    TestConsumer<String, String> consumer =
        this.testActors.consumer(this.topic, StringDeserializer.class, StringDeserializer.class);

    List<String> noise =
        IntStream.range(1, 1000)
            .mapToObj(i -> RandomStringUtils.insecure().nextAlphabetic(8))
            .toList();

    List<String> expected =
        List.of(
            RandomStringUtils.insecure().nextAlphabetic(8),
            RandomStringUtils.insecure().nextAlphabetic(8),
            RandomStringUtils.insecure().nextAlphabetic(8));

    List<String> input = this.shuffle(noise, expected);

    try (ExecutorService executorService = Executors.newSingleThreadExecutor()) {
      executorService.submit(
          () -> {
            input.stream().parallel().forEach(producer::send);
            producer.close();
          });
    }

    List<String> actual = consumer.consumeUntil(RecordPredicates.containsAll(expected));

    Assertions.assertThat(actual).doesNotHaveDuplicates();
  }

  @Test
  void shouldConsumeGenericRecord() throws Exception {
    Schema schema = new Schemas().load("user.avro");
    SchemaRegistrySubjects.register(this.topic, new AvroSchema(schema))
        .using(this.getSchemaRegistryClient());

    TestProducer<Bytes, Object> producer = this.testActors.producer(this.topic);
    TestConsumer<Bytes, GenericRecord> consumer = this.testActors.consumer(this.topic);

    String name = RandomStringUtils.insecure().nextAlphabetic(11);

    GenericData.Record genericRecord =
        new GenericRecordBuilder(schema)
            .set("name", name)
            .set("favorite_number", 7)
            .set("favorite_color", "blue")
            .build();

    producer.produce(genericRecord);
    GenericRecord actual = consumer.consumeUntilMatch(RecordPredicates.nameEquals(name));

    Assertions.assertThat(actual.toString())
        .hasToString(
            "{\"name\": \""
                + name
                + "\", "
                + "\"favorite_number\": 7, "
                + "\"favorite_color\": \"blue\"}");
  }

  @SafeVarargs
  private List<String> shuffle(List<String>... lists) {
    List<String> tmp = new ArrayList<>(Stream.of(lists).flatMap(Collection::stream).toList());

    Collections.shuffle(tmp);

    return tmp;
  }
}
