package com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TopicCreateCommand implements AdminClientCommand {

    private final Set<String> names;

    TopicCreateCommand(Set<String> names) {
        this.names = this.copyOf(names);
    }

    @Override
    public void using(AdminClient adminClient) throws Exception {
        this.names.removeAll(KafkaTopics.getNames().using(adminClient));

        adminClient.createTopics(this.toTopics(this.names))
                .all()
                .get();
    }

    private List<NewTopic> toTopics(Set<String> names) {
        return names.stream()
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();
    }

    private Set<String> copyOf(Set<String> input) {
        return input.stream()
                .map(String::new)
                .collect(Collectors.toSet());
    }
}
