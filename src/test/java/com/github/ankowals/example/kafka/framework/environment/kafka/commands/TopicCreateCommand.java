package com.github.ankowals.example.kafka.framework.environment.kafka.commands;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TopicCreateCommand implements AdminClientCommand {

    private final Set<String> names;

    private TopicCreateCommand(Set<String> names) {
        this.names = names;
    }

    public static TopicCreateCommand createTopics(String... names) {
        return new TopicCreateCommand(new HashSet<>(Arrays.asList(names)));
    }

    @Override
    public void run(AdminClient adminClient) throws Exception {
        names.removeAll(getTopics(adminClient));

        List<NewTopic> topics = names.stream()
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();

        adminClient.createTopics(topics).all().isDone();
    }

    private Set<String> getTopics(AdminClient adminClient) throws ExecutionException, InterruptedException, TimeoutException {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false);

        return adminClient.listTopics(options)
                .names()
                .get(5, TimeUnit.SECONDS);
    }
}
