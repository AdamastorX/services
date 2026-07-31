package com.adamastorx.watchlist.delivery;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Real delivery transport: the same ntfy.sh channel/mechanism backlog #21c
 * already proved works for Alertmanager (argocd/apps/prometheus.yaml's
 * webhook_configs) -- not a second notification path invented for
 * subscribers. Unlike Alertmanager's raw-JSON-blob POST (its own webhook
 * shape leaves no room to template), this is a plain text/plain body,
 * which ntfy renders as the message text directly -- confirmed against
 * ntfy's publish docs (a bare POST body with no special headers is treated
 * as the message).
 *
 * <p>Per-subscription topic ({@code SubscriptionEntity#ntfyTopic}) rather
 * than one hardcoded topic -- defaults to the same project topic
 * (app.ntfy.default-topic, matching prometheus.yaml's alerts topic
 * verbatim) unless a subscription overrides it, so the common case reuses
 * the one already-verified-live channel while still allowing a distinct
 * topic if ever needed.
 */
@Component
public class NtfyClient {

    private final RestClient restClient;

    // Found on review (same class of issue as OutboxRelay's Kafka send(), backlog
    // #53/api's outbox retrofit): RestClient.builder() alone has no connect/read
    // timeout at all, so a hung or unreachable ntfy.sh would block this relay's
    // single-threaded poll loop indefinitely instead of failing the attempt and
    // retrying on the next tick, same "one stuck call starves the whole batch"
    // shape. Explicit, generous-but-bounded timeouts close that.
    public NtfyClient(@Value("${app.ntfy.base-url}") String ntfyBaseUrl) {
        JdkClientHttpRequestFactory requestFactory =
                new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build());
        requestFactory.setReadTimeout(Duration.ofSeconds(5));
        this.restClient = RestClient.builder().baseUrl(ntfyBaseUrl).requestFactory(requestFactory).build();
    }

    /** POST {ntfyBaseUrl}/{topic} with the message as a plain-text body. Throws on
     * anything but a 2xx -- NotificationRelay treats any exception as a failed attempt. */
    public void send(String topic, String message) {
        restClient
                .post()
                .uri("/{topic}", topic)
                .contentType(MediaType.TEXT_PLAIN)
                .body(message)
                .retrieve()
                .toBodilessEntity();
    }
}
