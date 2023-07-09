package com.github.ankowals.example.kafka.actors;

import com.github.ankowals.example.kafka.framework.actors.TestActors;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Primary;
import io.micronaut.context.annotation.Property;
import jakarta.inject.Singleton;

@Factory
public class TestActorFactory {

    @Bean
    @Primary
    @Singleton
    public TestActors create(@Property(name = "kafka.bootstrap.servers") String bootstrapServer,
                             @Property(name = "kafka.schema.registry.url") String schemaRegistryUrl) {
        return new TestActors(bootstrapServer, schemaRegistryUrl);
    }
}
