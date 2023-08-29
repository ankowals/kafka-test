package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.IntegrationTestBase;
import com.github.ankowals.example.kafka.framework.actors.TestActors;
import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import com.github.ankowals.example.kafka.framework.environment.wiremock.RequestNumberAssertion;
import com.github.ankowals.example.kafka.mocks.ConfigureMock;
import com.github.ankowals.example.kafka.predicates.RecordPredicates;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@MicronautTest
public class StreamsTest extends IntegrationTestBase {

    @Inject
    private TestActors testActors;

    @Inject
    private KafkaStreams kafkaStreams;

    @BeforeEach
    void setupStreams() {
        WAIT.until(() -> this.kafkaStreams.state().equals(KafkaStreams.State.RUNNING));
    }

    @Test
    public void shouldFilterOutValues() throws Exception {
        TestProducer<String, String> producer = this.createProducer();
        TestConsumer<String, String> consumer = this.createConsumer();

        List<String> excluded = List.of("Zonk", "Terefere");

        ConfigureMock.filteringService()
                .excludedValues(excluded)
                .run(this.getFilteringServiceStub());

        List<String> expected = List.of(
                RandomStringUtils.randomAlphabetic(8),
                RandomStringUtils.randomAlphabetic(8),
                RandomStringUtils.randomAlphabetic(8));

        this.mergeAndShuffle(excluded, expected)
                .stream().parallel()
                .forEach(producer::send);

        producer.close();

        List<String> actual = consumer.consumeUntil(RecordPredicates.containsAll(expected));

        Assertions.assertThat(actual).doesNotHaveDuplicates();
        Assertions.assertThat(actual).doesNotContain(excluded.toArray(String[]::new));

        RequestNumberAssertion.assertThat(this.getFilteringServiceStub())
                .received(WireMock.moreThanOrExactly(1))
                .requestForEachStubPattern();
    }

    private List<String> mergeAndShuffle(List<String> list1, List<String> list2) {
        List<String> tmp = new ArrayList<>();
        tmp.addAll(list1);
        tmp.addAll(list2);

        Collections.shuffle(tmp);

        return tmp;
    }

    private TestProducer<String, String> createProducer() {
        return this.testActors.producer("word-input", StringSerializer.class, StringSerializer.class);
    }

    private TestConsumer<String, String> createConsumer() {
        return this.testActors.consumer("word-output", StringDeserializer.class, StringDeserializer.class);
    }
}
