package com.github.ankowals.example.kafka.framework.environment.wiremock;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class WireMockStub {

    private static final Logger LOGGER = LoggerFactory.getLogger(WireMockStub.class);

    private final WireMockServer server;

    private WireMockStub() {
        this.server = new WireMockServer(wireMockConfig().dynamicPort());
        this.server.addMockServiceRequestListener(WireMockStub::requestReceived);
    }

    public static WireMockStub start() {
        WireMockStub wireMockServerStub = new WireMockStub();
        wireMockServerStub.getServer().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> wireMockServerStub.getServer().stop()));

        return wireMockServerStub;
    }

    public static WireMockStub start(WireMockServerCommand wireMockServerCommand) {
        WireMockStub wireMockStub = start();

        try {
            wireMockServerCommand.run(wireMockStub.getServer());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return wireMockStub;
    }

    public void stop() {
        this.server.stop();
    }

    public WireMockServer getServer() {
        return server;
    }

    private static void requestReceived(Request request, Response response) {
        LOGGER.debug("WireMock request at URL: {}", request.getAbsoluteUrl());
        LOGGER.trace("WireMock request headers: \n{}", request.getHeaders());
        LOGGER.trace("WireMock response body: \n{}", response.getBodyAsString());
        LOGGER.trace("WireMock response headers: \n{}", response.getHeaders());
    }
}
