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
     * The best-known price/sentiment correlation for one ticker -- the
     * current window's data if it has any, otherwise the most recently
     * known value regardless of age (see {@code AggregateQueryService}'s
     * own javadoc; {@code priceAsOf}/{@code sentimentAsOf} on the
     * response say how stale it really is). {@code 503} while the app is
     * still restoring state after a restart (backlog #81's own AC
     * scenario -- a real, honest signal, not a generic error) rather than
     * a misleading {@code 404} that looks identical to "no data for this
     * ticker." {@code 404} now means this ticker has genuinely never had
     * a price tick since this process started, not merely "nothing in
     * the current window" -- a materially rarer real state than before.
     */
    @GetMapping("/aggregates/{ticker}")
    public ResponseEntity<TickerAggregateResponse> get(@PathVariable String ticker) {
        if (!queryService.isReady()) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "State store still restoring from the Kafka changelog");
        }
        return queryService
                .latestKnownState(ticker.toUpperCase(Locale.ROOT))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** The best-known aggregate for every watchlisted ticker (backlog #81's AC), batched -- see {@link #get} above. */
    @GetMapping("/aggregates")
    public ResponseEntity<List<TickerAggregateResponse>> getAll() {
        if (!queryService.isReady()) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE, "State store still restoring from the Kafka changelog");
        }
        return ResponseEntity.ok(queryService.latestKnownStateForWatchlist());
    }
}
