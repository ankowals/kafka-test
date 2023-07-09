package com.github.ankowals.example.kafka.framework.environment.kafka;

import com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin.AdminClientCommand;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class Kafka {

    private static final String FULL_IMAGE_NAME = "lensesio/fast-data-dev:2.5.1-L0";

    private final KafkaContainer container;
    private final AdminClient adminClient;
    private final SchemaRegistryClient schemaRegistryClient;

    private Kafka(DockerImageName dockerImageName) {
        this.container = this.createContainer(dockerImageName);
        this.container.start();

        this.adminClient = this.createAdminClient(this.container.getBootstrapServers());
        this.schemaRegistryClient = this.createSchemaRegistryClient(this.container.getSchemaRegistryUrl());
    }

    public static Kafka start() {
        return new Kafka(DockerImageName.parse(FULL_IMAGE_NAME));
    }

    public static Kafka start(AdminClientCommand adminClientCommand) {
        try {
            Kafka kafka = start();
            adminClientCommand.using(kafka.getAdminClient());

            return kafka;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public AdminClient getAdminClient() {
        return this.adminClient;
    }

    public SchemaRegistryClient getSchemaRegistryClient() { return this.schemaRegistryClient; }

    public KafkaContainer getContainer() {
        return this.container;
    }

    private KafkaContainer createContainer(DockerImageName dockerImageName) {
        return new KafkaContainer(dockerImageName).withReuse(true);
    }

    private AdminClient createAdminClient(String bootstrapServer) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        return AdminClient.create(props);
    }

    private SchemaRegistryClient createSchemaRegistryClient(String schemaRegistryUrl) {
        return new CachedSchemaRegistryClient(schemaRegistryUrl, 24);
    }
}
