package com.github.ankowals.example.kafka.framework.environment.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.NetworkSettings;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

public class KafkaContainer extends GenericContainer<KafkaContainer> {

  private int port = -1;

  public KafkaContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    this.withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093, BROKER://0.0.0.0:9092")
        .withEnv("PRE_SETUP_FILE", "/advertise_listeners.sh")
        .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT, BROKER:PLAINTEXT")
        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
        .withEnv("KAFKA_BROKER_ID", "1")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
        .withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", String.valueOf(Long.MAX_VALUE))
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "500")
        .withExposedPorts(3030, 8081, 9092, 9093)
        .waitingFor(
            Wait.forHttp("/subjects")
                .forPort(8081)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(1)));
  }

  public String getBootstrapServers() {
    if (this.port == -1) {
      throw new IllegalStateException("You should start Kafka container first!");
    }

    return String.format("PLAINTEXT://%s:%s", this.getHost(), this.port);
  }

  public String getSchemaRegistryUrl() {
    return String.format("http://%s:%s", this.getHost(), this.getMappedPort(8081));
  }

  @Override
  protected void doStart() {
    this.withCommand(
        "sh",
        "-c",
        "while [ ! -f /advertise_listeners.sh ]; do "
            + "sleep 0.1;"
            + "done;"
            + "/usr/local/bin/setup-and-run.sh");
    super.doStart();
  }

  @Override
  protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
    super.containerIsStarting(containerInfo, reused);
    this.port = this.getMappedPort(9093);
    this.copyFileToContainer(
        Transferable.of(this.createCommand(containerInfo).getBytes(StandardCharsets.UTF_8), 0777),
        "/advertise_listeners.sh");
  }

  private String createCommand(InspectContainerResponse containerInfo) {
    return String.join(
        "",
        "#!/bin/bash\n",
        "export KAFKA_ADVERTISED_LISTENERS='",
        this.getKafkaAdvertisedListeners(containerInfo),
        "'\n");
  }

  private String getKafkaAdvertisedListeners(InspectContainerResponse containerInfo) {
    return Stream.concat(
            Stream.of(this.getBootstrapServers()),
            Optional.ofNullable(containerInfo)
                .map(InspectContainerResponse::getNetworkSettings)
                .map(NetworkSettings::getNetworks)
                .map(Map::values)
                .stream()
                .flatMap(Collection::stream)
                .map(ContainerNetwork::getIpAddress)
                .map(this::convertToUrl))
        .collect(Collectors.joining(","));
  }

  private String convertToUrl(String ipAddress) {
    return String.format("BROKER://%s:9092", ipAddress);
  }
}
