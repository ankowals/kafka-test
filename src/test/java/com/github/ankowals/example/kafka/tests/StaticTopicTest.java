package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.actors.StaticTopicTestConsumer;
import com.github.ankowals.example.kafka.actors.StaticTopicTestProducer;
import com.github.ankowals.example.kafka.TestBase;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.github.ankowals.example.kafka.framework.actors.TestTopicCreateCommand.createTopics;
import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
public class StaticTopicTest extends TestBase {

    @Inject
    public StaticTopicTestProducer testProducer;

    @Inject
    public StaticTopicTestConsumer testConsumer;

    @BeforeAll
    void setup() throws Exception {
        createTopics(Set.of("testTopic")).run(getAdminClient());
    }

    @Test
    public void shouldConsumeProducedRecords() throws InterruptedException {
        testProducer.produce("terefere");
        String message = testConsumer.getRecord();

        assertThat(message).isEqualTo("terefere");
    }
}
