package com.github.ankowals.example.kafka;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

@Factory
public class WordStreamFactory {

    @Property(name="kafka.bootstrap.servers")
    String bootstrapServers;

    @Singleton
    KStream<String, String> wordFilteringStream(ConfiguredStreamBuilder builder, FilteringServiceApiClient client) {
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        KStream<String, String> source = builder.stream("word-input", Consumed.with(Serdes.String(), Serdes.String()));

        source.peek((key, value) -> System.out.println("Incoming record: " + value))
              .filter((key, value) -> !client.fetchExcluded().contains(value))
              .to("word-output", Produced.with(Serdes.String(), Serdes.String()));

        return source;
    }
}
