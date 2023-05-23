package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.actors.StaticTopicTestConsumer;
import com.github.ankowals.example.kafka.actors.StaticTopicTestProducer;
import com.github.ankowals.example.kafka.IntegrationTestBase;
import com.github.ankowals.example.kafka.framework.environment.kafka.commands.KafkaTopics;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@MicronautTest
public class StaticTopicTest extends IntegrationTestBase {

    @Inject
    public StaticTopicTestProducer testProducer;

    @Inject
    public StaticTopicTestConsumer testConsumer;

    @BeforeAll
    void createTopics() throws Exception {
        KafkaTopics.create("test-topic").run(this.getAdminClient());
    }

    @BeforeEach
    void clearBuffer() {
        this.testConsumer.clearBuffer();
    }

    @Test
    public void shouldConsumeProducedRecords() {
        String record = randomAlphabetic(11);

        this.testProducer.produce(record);

        await().untilAsserted(() -> {
                    List<String> records = this.testConsumer.getRecords();
                    assertThat(records).contains(record);
                });
    }
}
