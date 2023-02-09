package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.actors.StaticTopicTestConsumer;
import com.github.ankowals.example.kafka.actors.StaticTopicTestProducer;
import com.github.ankowals.example.kafka.base.TestBase;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public class StaticTopicTest extends TestBase {

    @Inject
    public StaticTopicTestProducer testProducer;

    @Inject
    public StaticTopicTestConsumer testConsumer;

    @BeforeAll
    void setup() throws ExecutionException, InterruptedException, TimeoutException {
        createTopic("testTopic");
    }

    @Test
    public void shouldConsumeProducedRecords() throws InterruptedException {
        testProducer.produce("terefere");
        String message = testConsumer.getRecord();

        assertThat(message).isEqualTo("terefere");
    }

    private void createTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
        if (!getTopics().contains(topic)) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

            getAdminClient().createTopics(List.of(newTopic))
                    .all()
                    .get(5, TimeUnit.SECONDS);
        }
    }

    private Set<String> getTopics() throws ExecutionException, InterruptedException, TimeoutException {
        return getAdminClient().listTopics(new ListTopicsOptions().listInternal(false))
                .names()
                .get(5, TimeUnit.SECONDS);
    }
}
