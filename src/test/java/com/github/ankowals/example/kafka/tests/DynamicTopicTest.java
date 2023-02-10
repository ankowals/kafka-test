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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public class DynamicTopicTest extends TestBase {

    private TestActorFactory actorFactory;
    private String topic;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException, TimeoutException {
        this.topic = randomAlphabetic(8);
        this.actorFactory = new TestActorFactory(getProperties().get("kafka.bootstrap.servers"));

        createTopic(topic);
    }

    @Test
    public void shouldConsumeProducedRecords() throws InterruptedException {
        Integer randomNumber = nextInt();

        TestProducer<String, Integer> producer = actorFactory.producer(StringSerializer.class, IntegerSerializer.class);
        TestConsumer<String, Integer> consumer = actorFactory.consumer(StringDeserializer.class, IntegerDeserializer.class);

        producer.produce(topic, randomNumber);
        List<Integer> message = consumer.consume(topic);

        assertThat(message.size()).isEqualTo(1);
        assertThat(message.get(message.size() - 1)).isEqualTo(randomNumber);
    }

    @Test
    public void shouldConsumeProducedRecordsUntilConditionIsFulfilled() throws InterruptedException {
        TestProducer<String, String> producer = actorFactory.producer(StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = actorFactory.consumer(StringDeserializer.class, StringDeserializer.class);

        Executors.newSingleThreadExecutor().submit(() -> {
            (IntStream.range(1, 2000))
                    .forEach(i -> producer.send(topic, "terefere-" + i));

            producer.close();
        });

        List<String> messages = consumer.consumeUntil(topic, list -> list.size() == 1999);

        assertThat(messages.size()).isEqualTo(1999);
        assertThat(messages).contains("terefere-1999");
    }

    private void createTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

        getAdminClient().createTopics(List.of(newTopic))
                .all()
                .get(5, TimeUnit.SECONDS);
    }
}
