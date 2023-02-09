package com.github.ankowals.example.kafka.environment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;
import java.util.Properties;

public class Kafka {

    private final KafkaContainer container;
    private final AdminClient adminClient;

    private Kafka(DockerImageName dockerImageName) throws SQLException {
        this.container = createContainer(dockerImageName);
        container.start();

        this.adminClient = createAdminClient(container.getBootstrapServers());
    }

    public static Kafka start() {
        try {
            return new Kafka(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public AdminClient getAdminClient() {
        return adminClient;
    }

    public KafkaContainer getContainer() {
        return container;
    }

    private KafkaContainer createContainer(DockerImageName dockerImageName) {
        try(KafkaContainer container = new KafkaContainer(dockerImageName)
                .withReuse(true)) {

            return container;
        }
    }

    private AdminClient createAdminClient(String bootstrapServer) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        return AdminClient.create(props);
    }
}
