package com.github.ankowals.example.kafka.framework.environment.kafka.commands;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KafkaCreate {

    public static AdminClientCommand topics(String... names) {
        return adminClient -> {
            Set<String> namesToCreate = new HashSet<>(Arrays.asList(names));
            Set<String> actualNames = KafkaGet.topicNames().run(adminClient);

            namesToCreate.removeAll(actualNames);

            adminClient.createTopics(mapToTopics(namesToCreate))
                    .all()
                    .get();
        };
    }

    private static List<NewTopic> mapToTopics(Set<String> namesSet) {
        return namesSet.stream()
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();
    }
}
