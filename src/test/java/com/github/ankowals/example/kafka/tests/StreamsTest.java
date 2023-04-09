package com.github.ankowals.example.kafka.tests;

import com.github.ankowals.example.kafka.IntegrationTestBase;
import com.github.ankowals.example.kafka.actors.TestActorFactory;
import com.github.ankowals.example.kafka.framework.actors.TestConsumer;
import com.github.ankowals.example.kafka.framework.actors.TestProducer;
import com.github.ankowals.example.kafka.mocks.FilteringServiceStub;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@MicronautTest
public class StreamsTest extends IntegrationTestBase {

    @Inject
    private TestActorFactory actorFactory;

    @Inject
    private KafkaStreams kafkaStreams;

    @BeforeEach
    void setupStreams() {
        await().until(() -> this.kafkaStreams.state().equals(KafkaStreams.State.RUNNING));
    }

    @Test
    public void shouldFilterOutValues() throws Exception {
        TestProducer<String, String> producer = this.actorFactory.producer("word-input", StringSerializer.class, StringSerializer.class);
        TestConsumer<String, String> consumer = this.actorFactory.consumer("word-output", StringDeserializer.class, StringDeserializer.class);

        List<String> excludedRecords = List.of("Zonk", "Terefere");

        FilteringServiceStub.configure()
                .excludedValues(excludedRecords)
                .run(this.getFilteringServiceStub());

        List<String> expectedRecords = List.of(randomAlphabetic(8),
                randomAlphabetic(8),
                randomAlphabetic(8));

        this.mergeAndShuffle(excludedRecords, expectedRecords)
                .stream().parallel()
                .forEach(producer::send);

        producer.close();

        List<String> actualRecords = consumer.consumeUntil(this.containsAll(expectedRecords));

        assertThat(actualRecords).doesNotHaveDuplicates();
        assertThat(actualRecords).doesNotContain(excludedRecords.toArray(String[]::new));
    }

    private Predicate<List<String>> containsAll(List<String> sublist) {
        return list-> list.containsAll(sublist);
    }

    private List<String> mergeAndShuffle(List<String> list1, List<String> list2) {
        List<String> tmp = new ArrayList<>();
        tmp.addAll(list1);
        tmp.addAll(list2);

        Collections.shuffle(tmp);

        return tmp;
    }
}
