package com.adamastorx.aggregator.api;

import java.util.List;
import java.util.Locale;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Backlog #81's AC: "a small, plain REST query API; no gold-plated
 * streaming/GraphQL API for v1" -- two {@code GET}s, no request body, no
 * query params, matching {@code api}'s own simplest lookup endpoints
 * ({@code VariantLookupController}). This is what #82 ({@code
 * visualizer}) reads from.
 */
@RestController
public class AggregateController {

    private final AggregateQueryService queryService;

    public AggregateController(AggregateQueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * The current window's price/sentiment correlation for one ticker.
     * {@code 503} while the app is still restoring state after a restart
     * (backlog #81's own AC scenario -- a real, honest signal, not a
     * generic error) rather than a misleading {@code 404} that looks
     * identical to "no data for this ticker."
     */
    @GetMapping("/aggregates/{ticker}")
    public ResponseEntity<TickerAggregateResponse> get(@PathVariable String ticker) {
        if (!queryService.isReady()) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "State store still restoring from the Kafka changelog");
        }
        return queryService
                .currentWindow(ticker.toUpperCase(Locale.ROOT))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** The current window's aggregate for every watchlisted ticker (backlog #81's AC), batched. */
    @GetMapping("/aggregates")
    public ResponseEntity<List<TickerAggregateResponse>> getAll() {
        if (!queryService.isReady()) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "State store still restoring from the Kafka changelog");
        }
        return ResponseEntity.ok(queryService.currentWindowForWatchlist());
    }
}
