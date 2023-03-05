package com.github.ankowals.example.kafka.framework.environment.kafka.commands;

import org.apache.kafka.clients.admin.AdminClient;

@FunctionalInterface
public interface AdminClientCommand {
    void run(AdminClient adminClient) throws Exception;
}
