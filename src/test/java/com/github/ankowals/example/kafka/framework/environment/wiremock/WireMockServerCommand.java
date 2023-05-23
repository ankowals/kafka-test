package com.github.ankowals.example.kafka.framework.environment.wiremock;

import com.github.tomakehurst.wiremock.WireMockServer;

@FunctionalInterface
public interface WireMockServerCommand {
    void run(WireMockServer wireMockServer) throws Exception;
}
