package com.github.ankowals.example.kafka.framework.environment.kafka.commands;

import org.apache.kafka.clients.admin.AdminClient;

@FunctionalInterface
public interface AdminClientQuery<T> {
    T run(AdminClient adminClient) throws Exception;
}
