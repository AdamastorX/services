package com.adamastorx.watchlist.delivery;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
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

    public NtfyClient(@Value("${app.ntfy.base-url}") String ntfyBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(ntfyBaseUrl).build();
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
