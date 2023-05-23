package com.github.ankowals.example.kafka.framework.environment.kafka.commands;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Set;

public class CreateKafkaTopicCommand implements AdminClientCommand {

    private final Set<String> names;

    CreateKafkaTopicCommand(Set<String> names) {
        this.names = names;
    }

    @Override
    public void run(AdminClient adminClient) throws Exception {
        Set<String> actualNames = KafkaTopics.getNames().run(adminClient);

        this.names.removeAll(actualNames);

        adminClient.createTopics(this.mapToTopics(this.names))
                .all()
                .get();
    }

    private List<NewTopic> mapToTopics(Set<String> names) {
        return names.stream()
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();
    }
}
