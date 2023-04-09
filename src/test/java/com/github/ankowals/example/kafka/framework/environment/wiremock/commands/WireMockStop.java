package com.github.ankowals.example.kafka.framework.environment.wiremock.commands;

public class WireMockStop {
    public static WireMockServerCommand atShutdown() {
        return wireMockServer -> Runtime.getRuntime().addShutdownHook(new Thread(wireMockServer::stop));
    }
}
