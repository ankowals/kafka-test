package com.github.ankowals.example.kafka.environment;

import com.github.ankowals.example.kafka.framework.environment.wiremock.WireMockStub;
import com.github.ankowals.example.kafka.mocks.FilteringServiceStub;
import com.github.tomakehurst.wiremock.WireMockServer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface UsesFilteringServiceStub {

    WireMockStub FILTERING_SERVICE_STUB_INSTANCE = WireMockStub.start(
            FilteringServiceStub.configure().excludedValues(List.of("Zonk", "Kwak", "Hop"))
    );

    default WireMockServer getFilteringServiceStub() { return FILTERING_SERVICE_STUB_INSTANCE.getServer(); }

    default Map<String, String> getFilteringServiceProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("micronaut.http.services.filtering-service.url", FILTERING_SERVICE_STUB_INSTANCE.getServer().baseUrl());

        return properties;
    }
}
