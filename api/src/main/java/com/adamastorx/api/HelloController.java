package com.adamastorx.api;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Trivial endpoint (services#2), originally added to prove the gateway
 * forwarding path end to end (ADR 0010). `gateway` was removed under ADR
 * 0021 — `api` is now reached directly via its own Ingress — this endpoint
 * remains as-is. No business logic here yet — that arrives in future
 * issues.
 */
@RestController
public class HelloController {

    @GetMapping("/hello")
    public Map<String, String> hello() {
        return Map.of("message", "hello from api");
    }
}
