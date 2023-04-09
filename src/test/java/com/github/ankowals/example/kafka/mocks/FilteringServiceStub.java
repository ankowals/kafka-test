package com.github.ankowals.example.kafka.mocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.ankowals.example.kafka.framework.environment.wiremock.commands.WireMockServerCommand;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.micronaut.http.HttpStatus;
import wiremock.org.apache.hc.core5.http.ContentType;

import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

public class FilteringServiceStub implements WireMockServerCommand {

    private final ObjectWriter objectWriter;
    private List<String> excludedValues;

    private FilteringServiceStub() {
        this.objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
        this.excludedValues = new ArrayList<>();
    }

    public static FilteringServiceStub configure() {
        return new FilteringServiceStub();
    }

    public FilteringServiceStub excludedValues(List<String> values) {
        this.excludedValues = values;
        return this;
    }

    @Override
    public void run(WireMockServer wireMockServer) throws Exception {
        wireMockServer.resetAll();
        wireMockServer.stubFor(this.get("/excluded-values")
                .willReturn(response(this.objectWriter.writeValueAsString(this.excludedValues))));
    }

    private MappingBuilder get(String path) {
        return WireMock.get(urlPathMatching(path));
    }

    private ResponseDefinitionBuilder response(String body) {
        return aResponse().withStatus(HttpStatus.OK.getCode())
                .withHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType())
                .withBody(body);
    }
}
