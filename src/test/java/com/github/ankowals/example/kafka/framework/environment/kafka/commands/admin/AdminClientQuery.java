package com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin;

import org.apache.kafka.clients.admin.AdminClient;

@FunctionalInterface
public interface AdminClientQuery<T> {
    T using(AdminClient adminClient) throws Exception;
}
