package com.github.ankowals.example.kafka.environment;

import com.github.ankowals.example.kafka.framework.environment.wiremock.WireMockServerStub;
import com.github.tomakehurst.wiremock.WireMockServer;

public interface Uses3rdPartyServerStub {

    WireMockServerStub THIRD_PARTY_SERVER_INSTANCE = WireMockServerStub.start();

    default WireMockServer get3rdPartyServerStub() { return THIRD_PARTY_SERVER_INSTANCE.getServer(); }
}
