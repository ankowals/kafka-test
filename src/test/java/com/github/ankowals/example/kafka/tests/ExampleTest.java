package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.actors.CustomTestConsumer;
import com.github.ankowals.example.kafka.actors.TestActorFactory;
import com.github.ankowals.example.kafka.actors.TestConsumer;
import com.github.ankowals.example.kafka.actors.TestProducer;
import com.github.ankowals.example.kafka.base.TestBase;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public class ExampleTest extends TestBase {

    @Inject
    public TestProducer testProducer;

    @Inject
    public TestConsumer testConsumer;

    TestActorFactory testActorFactory;

    @BeforeAll
    void setup() {
        createTopics("testTopic");
        testActorFactory = new TestActorFactory(getProperties().get("kafka.bootstrap.servers"));
    }

    @Test
    public void shouldConsumeProducedRecords() throws InterruptedException {
        testProducer.produce("Hello");
        String bodyOfMessage = testConsumer.getRecord();
        assertThat(bodyOfMessage).isEqualTo("Hello");
    }

    @Test
    public void shouldConsumeProducedRecordsUsingActorsFromFactory() throws InterruptedException {
        CustomTestConsumer<String, String> consumer = testActorFactory.createConsumer();
        consumer.subscribe("testTopic");

        testProducer.produce("Hello");

        List<String> bodiesOfMessage = consumer.getRecordsAndCancel();
        assertThat(bodiesOfMessage.size()).isGreaterThanOrEqualTo(1);
        assertThat(bodiesOfMessage.get(bodiesOfMessage.size() - 1)).isEqualTo("Hello");
    }

    @Test
    public void shouldConsumeProducedRecordsUntilConditionIsFulfilled() throws InterruptedException {
        CustomTestConsumer<String, String> consumer = testActorFactory.createConsumer();
        consumer.subscribe("testTopic");

        IntStream.of(1,2,3).parallel()
                .forEach(i -> testProducer.produce("Hello-" + i));

        List<String> bodiesOfMessage = consumer.consumeUntil(list -> list.size() >= 3);
        assertThat(bodiesOfMessage.size()).isGreaterThanOrEqualTo(3);
        assertThat(bodiesOfMessage.get(bodiesOfMessage.size() - 1)).isEqualTo("Hello-3");
    }

    private void createTopics(String... topics) {
            List<NewTopic> topicList = Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();

            getAdminClient().createTopics(topicList);
    }

}
