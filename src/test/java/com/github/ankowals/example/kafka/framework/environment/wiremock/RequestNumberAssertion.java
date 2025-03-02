package com.github.ankowals.example.kafka.framework.environment.wiremock;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.CountMatchingStrategy;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.util.List;

public class RequestNumberAssertion {

  private final WireMockServer wireMockServer;

  private CountMatchingStrategy countMatchingStrategy;

  private RequestNumberAssertion(WireMockServer wireMockServer) {
    this.wireMockServer = wireMockServer;
  }

  public static RequestNumberAssertion assertThat(WireMockServer wireMockServer) {
    return new RequestNumberAssertion(wireMockServer);
  }

  public RequestNumberAssertion received(CountMatchingStrategy countMatchingStrategy) {
    this.countMatchingStrategy = countMatchingStrategy;
    return this;
  }

  public void requestForEachStubPattern() {
    List<StubMapping> stubMappings = this.wireMockServer.getStubMappings();
    stubMappings.forEach(
        stubMapping ->
            this.wireMockServer.verify(
                this.countMatchingStrategy,
                RequestPatternBuilder.forCustomMatcher(stubMapping.getRequest())));
  }
}
