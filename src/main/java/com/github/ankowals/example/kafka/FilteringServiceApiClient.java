package com.github.ankowals.example.kafka;

import static io.micronaut.http.HttpHeaders.ACCEPT;
import static io.micronaut.http.HttpHeaders.USER_AGENT;

import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Header;
import io.micronaut.http.client.annotation.Client;
import java.util.List;

@Client(id = "filtering-service")
@Header(name = USER_AGENT, value = "Micronaut HTTP Client")
@Header(name = ACCEPT, value = "application/json")
public interface FilteringServiceApiClient {
  @Get("/excluded-values")
  List<String> fetchExcluded();
}
