package com.github.ankowals.example.kafka.framework.actors;

import com.github.ankowals.example.kafka.framework.environment.AdminClientCommand;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TestTopicCreateCommand implements AdminClientCommand {

    private final Set<String> names;

    private TestTopicCreateCommand(Set<String> names) {
        this.names = names;
    }

    public static TestTopicCreateCommand createTopics(Set<String> names) {
        return new TestTopicCreateCommand(names);
    }

    @Override
    public void run(AdminClient adminClient) throws Exception {
        names.removeAll(getTopics(adminClient));

        List<NewTopic> topics = names.stream()
                .map(name -> new NewTopic(name, 1, (short) 1))
                .toList();

        createTopics(topics, adminClient);
    }

    private void createTopics(List<NewTopic> topics, AdminClient adminClient) throws ExecutionException, InterruptedException {
        adminClient.createTopics(topics)
                .all()
                .get();
    }

    private Set<String> getTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(false);

        return adminClient.listTopics(options)
                .names()
                .get();
    }
}
