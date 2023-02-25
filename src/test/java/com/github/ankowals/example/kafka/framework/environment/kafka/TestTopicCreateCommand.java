package com.github.ankowals.example.kafka.framework.environment.kafka;

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

public class TestTopicCreateCommand implements AdminClientCommand {

    private final Set<String> names;

    private TestTopicCreateCommand(Set<String> names) {
        this.names = names;
    }

    public static TestTopicCreateCommand createTopics(String... names) {
        return new TestTopicCreateCommand(new HashSet<>(Arrays.asList(names)));
    }

    @Override
    public void run(AdminClient adminClient) throws Exception {
        names.removeAll(getTopics(adminClient));

        List<NewTopic> topics = names.stream()
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();

        createTopics(topics, adminClient);
    }

    private void createTopics(List<NewTopic> topics, AdminClient adminClient) throws ExecutionException, InterruptedException, TimeoutException {
        adminClient.createTopics(topics)
                .all()
                .get(5, TimeUnit.SECONDS);
    }

    private Set<String> getTopics(AdminClient adminClient) throws ExecutionException, InterruptedException, TimeoutException {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false);

        return adminClient.listTopics(options)
                .names()
                .get(5, TimeUnit.SECONDS);
    }
}
