package com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin;

import org.apache.kafka.clients.admin.ListTopicsOptions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KafkaTopics {

    public static AdminClientCommand create(String... names) {
        return new TopicCreateCommand(new HashSet<>(Arrays.asList(names)));
    }

    public static AdminClientCommand delete(String... names) {
        return new TopicDeleteCommand(new HashSet<>(Arrays.asList(names)));
    }

    public static AdminClientQuery<Set<String>> getNames() {
        return adminClient -> adminClient.listTopics(new ListTopicsOptions().listInternal(false))
                .names()
                .get();
    }
}
