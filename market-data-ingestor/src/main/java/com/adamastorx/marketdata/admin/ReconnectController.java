package com.adamastorx.marketdata.admin;

import com.adamastorx.marketdata.finnhub.FinnhubWebSocketClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operational/test hook for backlog #78's own AC: reconnect-and-resume
 * must be "proven live by killing the connection and confirming ticks
 * resume without a pod restart, not proven once and assumed durable."
 *
 * <p>Real constraints this project actually has, stated rather than
 * glossed: there is no raw socket-level way to sever this specific TCP
 * connection from outside the process for this live proof. The runtime
 * image is {@code eclipse-temurin:25-jre-alpine} with no {@code ss}/
 * {@code iproute2} and no {@code CAP_NET_ADMIN} (Dockerfile), so an
 * in-pod {@code ss --kill} is not available; this cluster's CNI is
 * flannel (k3s default), which does not enforce {@code NetworkPolicy}, so
 * a policy meant to block egress to Finnhub would be a silent no-op, not
 * a real block -- worse than not testing at all if trusted as if it were
 * one.
 *
 * <p>This endpoint calls {@link FinnhubWebSocketClient#forceReconnect()}
 * directly -- the exact same {@code abort()}-then-{@code
 * scheduleReconnect()} path {@link FinnhubWebSocketClient}'s own
 * watchdog uses for a genuinely silent connection loss, and the same
 * {@code connect()}/{@code subscribeAll()} code {@code onClose}/{@code
 * onError} fall back to for a peer-reported drop. Triggering it here
 * proves that real code path actually reconnects and resubscribes on a
 * live, running instance, not a unit test with a mocked socket.
 *
 * <p>No Kubernetes {@code Service} exists for this port (ADR 0009/0011,
 * same as {@code workers}) -- reached only via {@code kubectl exec} +
 * {@code curl localhost:8080} or a port-forward, an internal operator
 * action, not a public API.
 */
@RestController
public class ReconnectController {

    private final FinnhubWebSocketClient finnhubWebSocketClient;

    public ReconnectController(FinnhubWebSocketClient finnhubWebSocketClient) {
        this.finnhubWebSocketClient = finnhubWebSocketClient;
    }

    @PostMapping("/internal/market-data/force-reconnect")
    public ResponseEntity<Void> forceReconnect() {
        finnhubWebSocketClient.forceReconnect();
        return ResponseEntity.accepted().build();
    }
}
