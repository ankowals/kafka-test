package com.github.ankowals.example.kafka.framework.environment.kafka.commands;

import org.apache.kafka.clients.admin.ListTopicsOptions;
import java.util.Set;

public class KafkaGet {

    public static AdminClientQuery<Set<String>> topicNames() {
        return adminClient -> adminClient.listTopics(new ListTopicsOptions().listInternal(false))
                    .names()
                    .get();
    }
}
