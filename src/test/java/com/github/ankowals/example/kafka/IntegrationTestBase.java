package com.github.ankowals.example.kafka;

import com.github.ankowals.example.kafka.environment.UsesFilteringServiceStub;
import com.github.ankowals.example.kafka.environment.UsesKafka;
import io.github.glytching.junit.extension.watcher.WatcherExtension;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.test.support.TestPropertyProvider;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WatcherExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class IntegrationTestBase
    implements UsesKafka, UsesFilteringServiceStub, TestPropertyProvider {

  protected ConditionFactory AWAIT =
      Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(100, TimeUnit.MILLISECONDS);

  @NonNull
  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.putAll(this.getKafkaProperties());
    properties.putAll(this.getFilteringServiceProperties());

    return properties;
  }
}
