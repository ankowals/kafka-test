package com.github.ankowals.example.kafka.framework.environment.wiremock;

import com.github.tomakehurst.wiremock.WireMockServer;

@FunctionalInterface
public interface ConfigureWireMockStubCommand {
    void configure(WireMockServer wireMockServer) throws Exception;
}
