package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.actors.TestConsumer;
import com.github.ankowals.example.kafka.actors.TestActorFactory;
import com.github.ankowals.example.kafka.actors.TestProducer;
import com.github.ankowals.example.kafka.base.TestBase;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public class DynamicTopicTest extends TestBase {

    private TestActorFactory testActorFactory;
    private String topic;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException, TimeoutException {
        this.topic = randomAlphabetic(8);
        this.testActorFactory = new TestActorFactory(getProperties().get("kafka.bootstrap.servers"));

        createTopic(topic);
    }

    @Test
    public void shouldConsumeProducedRecords() throws InterruptedException {
        Integer randomNumber = nextInt();

        TestProducer<String, Integer> producer = testActorFactory.createProducer(StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = testActorFactory.createConsumer(StringDeserializer.class, IntegerDeserializer.class);

        consumer.subscribe(topic);
        producer.produceAndClose(topic, randomNumber);
        List<Integer> bodiesOfMessage = consumer.consumeAndClose();

        assertThat(bodiesOfMessage.size()).isEqualTo(1);
        assertThat(bodiesOfMessage.get(bodiesOfMessage.size() - 1)).isEqualTo(randomNumber);
    }

    @Test
    public void shouldConsumeProducedRecordsUntilConditionIsFulfilled() throws InterruptedException {
        TestProducer<String, String> producer = testActorFactory.createProducer(StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = testActorFactory.createConsumer(StringDeserializer.class, StringDeserializer.class);

        consumer.subscribe(topic);

        (IntStream.range(1, 1000)).parallel()
                .forEach(i -> producer.produce(topic, "terefere-" + i));

        producer.close();
        List<String> bodiesOfMessage = consumer.consumeUntil(list -> list.size() == 999);

        assertThat(bodiesOfMessage.size()).isEqualTo(999);
        assertThat(bodiesOfMessage).contains("terefere-999");
    }

    private void createTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

        getAdminClient().createTopics(List.of(newTopic))
                .all()
                .get(5, TimeUnit.SECONDS);
    }
}
