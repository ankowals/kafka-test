package com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Set;
import java.util.stream.Collectors;

public class TopicDeleteCommand implements AdminClientCommand {

    private final Set<String> names;

    TopicDeleteCommand(Set<String> names) {
        this.names = this.copyOf(names);
    }

    @Override
    public void using(AdminClient adminClient) throws Exception {
        this.names.removeAll(KafkaTopics.getNames().using(adminClient));

        adminClient.deleteTopics(this.names)
                .all()
                .get();
    }

    private Set<String> copyOf(Set<String> input) {
        return input.stream()
                .map(String::new)
                .collect(Collectors.toSet());
    }
}
