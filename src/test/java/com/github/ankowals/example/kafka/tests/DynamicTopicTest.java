package com.github.ankowals.example.kafka.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.github.ankowals.example.kafka.User;
import com.github.ankowals.example.kafka.actors.TestConsumer;
import com.github.ankowals.example.kafka.actors.TestActorFactory;
import com.github.ankowals.example.kafka.actors.TestProducer;
import com.github.ankowals.example.kafka.base.TestBase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

public class DynamicTopicTest extends TestBase {

    private TestActorFactory actorFactory;
    private String topic;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException, TimeoutException {
        this.topic = randomAlphabetic(8);
        this.actorFactory = new TestActorFactory(
                getProperties().get("kafka.bootstrap.servers"),
                getProperties().get("kafka.schema.registry.url"));

        createTopic(topic);
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

        Integer message = getLast(consumer.consumeUntil(list -> list.contains(999)));

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
    public void shouldConsumeGenericRecord() throws IOException {
        TestProducer<Bytes, Object> producer = actorFactory.producer(topic);
        TestConsumer<Bytes, GenericRecord> consumer = actorFactory.consumer(topic);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("/user.avro"));

        GenericData.Record record = new GenericRecordBuilder(schema)
                .set("name", "John")
                .set("favorite_number", 7)
                .set("favorite_color", "blue")
                .build();

        producer.produce(record);
        GenericRecord message = getLast(consumer.consume());

        assertThat(message.toString())
                .isEqualTo("{\"name\": \"John\", \"favorite_number\": 7, \"favorite_color\": \"blue\"}");

        User actual = new ObjectMapper().readValue(String.valueOf(message), User.class);

        assertThat(actual.getName()).isEqualTo("John");
        assertThat(actual.getFavorite_color()).isEqualTo("blue");
        assertThat(actual.getFavorite_number()).isEqualTo(7);
    }

    @Test
    public void shouldConvertToGenericRecord() throws IOException {
        TestProducer<Bytes, Object> producer = actorFactory.producer(topic);
        TestConsumer<Bytes, GenericRecord> consumer = actorFactory.consumer(topic);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("/user.avro"));

        User user = new User("Joe", 1, "red");

        //GenericRecord record = mapToGenericRecord(user, schema);
        GenericRecord record = toGenericRecord(user, schema);

        producer.produce(record);
        GenericRecord message = getLast(consumer.consume());

        assertThat(message.toString())
                .isEqualTo("{\"name\": \"Joe\", \"favorite_number\": 1, \"favorite_color\": \"red\"}");

        User actual = new ObjectMapper().readValue(String.valueOf(message), User.class);

        assertThat(actual.getName()).isEqualTo("Joe");
        assertThat(actual.getFavorite_color()).isEqualTo("red");
        assertThat(actual.getFavorite_number()).isEqualTo(1);
    }

    private void createTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

        getAdminClient().createTopics(List.of(newTopic))
                .all()
                .get(5, TimeUnit.SECONDS);
    }

    private <T> T getLast(List<T> list) {
        return list != null && !list.isEmpty() ? list.get(list.size() - 1) : null;
    }

    private Predicate<List<Integer>> anyRecordFound() {
        return list -> list.size() > 0;
    }

    private Predicate<List<String>> numberOfRecordsIs(int number) {
        return list -> list.size() == number;
    }

    //using jackson mapper
    private <T> GenericRecord mapToGenericRecord(T object, Schema schema) throws IOException {
        final byte[] bytes = new AvroMapper().writer(new AvroSchema(schema)).writeValueAsBytes(object);
        GenericDatumReader<Object> genericRecordReader = new GenericDatumReader<>(schema);

        return (GenericRecord) genericRecordReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }

    //when object properties have same names as in avro schema
    private <T> GenericRecord toGenericRecord(T object, Schema schema) throws IOException {
        ReflectDatumWriter<T> reflectDatumWriter = new ReflectDatumWriter<>(schema);
        GenericDatumReader<Object> genericRecordReader = new GenericDatumReader<>(schema);

        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            reflectDatumWriter.write(object, EncoderFactory.get().directBinaryEncoder(bytes, null));
            return (GenericRecord) genericRecordReader.read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
        }
    }
}
