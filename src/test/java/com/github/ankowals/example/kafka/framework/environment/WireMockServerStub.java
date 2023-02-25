package com.github.ankowals.example.kafka.framework.environment;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class WireMockServerStub {

    private static final Logger LOGGER = LoggerFactory.getLogger(WireMockServerStub.class);

    private final WireMockServer server;

    private WireMockServerStub() {
        this.server = new WireMockServer(wireMockConfig().dynamicPort());
        this.server.addMockServiceRequestListener(WireMockServerStub::requestReceived);
    }

    public static WireMockServerStub start() {
        WireMockServerStub wireMockServerStub = new WireMockServerStub();
        wireMockServerStub.getServer().start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> wireMockServerStub.getServer().stop()));

        return wireMockServerStub;
    }

    public static WireMockServerStub start(ConfigureWireMockStubCommand configureWireMockStubCommand) {
        WireMockServerStub wireMockServerStub = start();

        try {
            configureWireMockStubCommand.configure(wireMockServerStub.getServer());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return wireMockServerStub;
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
