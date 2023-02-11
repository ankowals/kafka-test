package com.github.ankowals.example.kafka.actors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TopicCreator {

    private final AdminClient adminClient;

    public TopicCreator(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    public void create(String name) throws ExecutionException, InterruptedException, TimeoutException {
        NewTopic newTopic = new NewTopic(name, 1, (short) 1);

        adminClient.createTopics(List.of(newTopic))
                .all()
                .get(5, TimeUnit.SECONDS);
    }
}
