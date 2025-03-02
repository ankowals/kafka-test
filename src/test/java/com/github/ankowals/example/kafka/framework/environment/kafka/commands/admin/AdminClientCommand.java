package com.github.ankowals.example.kafka.framework.environment.kafka.commands.admin;

import org.apache.kafka.clients.admin.AdminClient;

@FunctionalInterface
public interface AdminClientCommand {
  void using(AdminClient adminClient) throws Exception;
}
