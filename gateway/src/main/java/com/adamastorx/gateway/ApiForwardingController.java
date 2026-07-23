package com.adamastorx.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

/**
 * Hand-rolled forwarding controller for gateway -> api routing (ADR 0010).
 * Uses Spring's blocking {@link RestClient} (already on the classpath via
 * spring-boot-starter-webmvc, no new dependency) rather than Spring Cloud
 * Gateway, which is built on WebFlux/Reactor Netty and would conflict with
 * gateway's servlet/Tomcat stack.
 */
@RestController
public class ApiForwardingController {

    private final RestClient restClient;

    public ApiForwardingController(@Value("${api.base-url}") String apiBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(apiBaseUrl).build();
    }

    @GetMapping("/api/hello")
    public String hello() {
        return restClient.get()
                .uri("/hello")
                .retrieve()
                .body(String.class);
    }
}
