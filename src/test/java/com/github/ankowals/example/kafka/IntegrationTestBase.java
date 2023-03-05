package com.github.ankowals.example.kafka;

import com.github.ankowals.example.kafka.environment.UsesKafka;
import io.github.glytching.junit.extension.watcher.WatcherExtension;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(WatcherExtension.class)
public class IntegrationTestBase implements UsesKafka, TestPropertyProvider {

    @NonNull
    @Override
    public Map<String, String> getProperties() {
        return new HashMap<>(getKafkaProperties());
    }
}
