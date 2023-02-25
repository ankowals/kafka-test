package com.github.ankowals.example.kafka.framework.environment;

import org.apache.kafka.clients.admin.AdminClient;

@FunctionalInterface
public interface AdminClientCommand {
    void run(AdminClient adminClient) throws Exception;
}
