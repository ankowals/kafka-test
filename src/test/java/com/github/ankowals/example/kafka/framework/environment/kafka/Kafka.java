package com.github.ankowals.example.kafka.framework.environment.kafka;

import com.github.ankowals.example.kafka.framework.environment.kafka.commands.AdminClientCommand;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class Kafka {

    private final KafkaContainer container;
    private final AdminClient adminClient;

    private Kafka(DockerImageName dockerImageName) {
        this.container = createContainer(dockerImageName);
        this.container.start();
        this.adminClient = createAdminClient(this.container.getBootstrapServers());
    }

    public static Kafka start() {
        return new Kafka(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));
    }

    public static Kafka start(AdminClientCommand adminClientCommand) {
        try {
            Kafka kafka = start();
            adminClientCommand.run(kafka.getAdminClient());

            return kafka;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public AdminClient getAdminClient() {
        return this.adminClient;
    }

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
}
