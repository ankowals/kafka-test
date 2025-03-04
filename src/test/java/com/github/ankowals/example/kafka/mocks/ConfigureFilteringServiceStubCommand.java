package com.github.ankowals.example.kafka.mocks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.ankowals.example.kafka.framework.environment.wiremock.WireMockServerCommand;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.micronaut.http.HttpStatus;
import java.util.ArrayList;
import java.util.List;
import wiremock.org.apache.hc.core5.http.ContentType;

public class ConfigureFilteringServiceStubCommand implements WireMockServerCommand {

  private final ObjectWriter objectWriter;
  private List<String> excludedValues;

  ConfigureFilteringServiceStubCommand() {
    this.objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
    this.excludedValues = new ArrayList<>();
  }

  public ConfigureFilteringServiceStubCommand excludedValues(List<String> values) {
    this.excludedValues = values;
    return this;
  }

  @Override
  public void run(WireMockServer wireMockServer) throws Exception {
    wireMockServer.resetAll();
    wireMockServer.stubFor(
        this.get("/excluded-values")
            .willReturn(this.response(this.objectWriter.writeValueAsString(this.excludedValues))));
  }

  private MappingBuilder get(String path) {
    return WireMock.get(WireMock.urlPathMatching(path));
  }

  private ResponseDefinitionBuilder response(String body) {
    return WireMock.aResponse()
        .withStatus(HttpStatus.OK.getCode())
        .withHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType())
        .withBody(body);
  }
}
