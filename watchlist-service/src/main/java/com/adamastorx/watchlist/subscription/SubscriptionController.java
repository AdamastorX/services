package com.adamastorx.watchlist.subscription;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Subscription CRUD (backlog #53's own AC). {@code variantKey} must be
 * supplied in the exact format clinvar.ingestion.completed's {@code
 * changedKeys} entries already use ({@code
 * "variantAnnotation:{chrom}:{pos}:{ref}:{alt}"}, VariantAnnotationCacheService#key's
 * format in the api module) -- reused verbatim rather than inventing a
 * second variant-identity format, so DeliveryResolutionService's match is a
 * plain equality lookup.
 *
 * <p>Exactly one of {@code variantKey}/{@code geneSymbol} may be set (V1
 * migration's CHECK constraint enforces this at the DB level too, this is
 * the friendlier 400 in front of it). {@code geneSymbol} subscriptions are
 * accepted and stored but never currently matched by a real event -- see
 * V1's migration comment and the watchlist-service README for why (no gene
 * data anywhere in clinvar-service's model today).
 */
@RestController
public class SubscriptionController {

    private final SubscriptionJpaRepository repository;
    private final String defaultNtfyTopic;

    public SubscriptionController(
            SubscriptionJpaRepository repository,
            @Value("${app.ntfy.default-topic}") String defaultNtfyTopic) {
        this.repository = repository;
        this.defaultNtfyTopic = defaultNtfyTopic;
    }

    @PostMapping("/subscriptions")
    public ResponseEntity<Subscription> create(@RequestBody Map<String, String> body) {
        String variantKey = body.get("variantKey");
        String geneSymbol = body.get("geneSymbol");
        if ((variantKey == null) == (geneSymbol == null)) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Provide exactly one of variantKey or geneSymbol");
        }
        // ntfy #21c's already-proven channel, not a second one, unless a caller
        // deliberately overrides it -- see NtfyClient's javadoc.
        String ntfyTopic = body.getOrDefault("ntfyTopic", defaultNtfyTopic);

        SubscriptionEntity entity = new SubscriptionEntity(UUID.randomUUID(), variantKey, geneSymbol, ntfyTopic, Instant.now());
        repository.save(entity);
        return ResponseEntity.status(HttpStatus.CREATED).body(toSubscription(entity));
    }

    @GetMapping("/subscriptions/{id}")
    public ResponseEntity<Subscription> get(@PathVariable UUID id) {
        return repository
                .findById(id)
                .map(SubscriptionController::toSubscription)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/subscriptions")
    public List<Subscription> list() {
        return repository.findAll().stream().map(SubscriptionController::toSubscription).toList();
    }

    @DeleteMapping("/subscriptions/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) {
        if (!repository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        // backlog #141: real deliveries reference this row
        // (deliveries_subscription_id_fkey) once a real fan-out has
        // happened -- found live deleting a real test subscription
        // during backlog #123's acceptance walk-through, a raw 500
        // instead of a real, callable-facing 409. The delivery history
        // is kept deliberately (it's the durable record a real
        // notification was sent), so a subscription with real
        // deliveries can't just be deleted out from under them.
        try {
            repository.deleteById(id);
        } catch (DataIntegrityViolationException ex) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT,
                    "Subscription has real delivery history and cannot be deleted",
                    ex);
        }
        return ResponseEntity.noContent().build();
    }

    private static Subscription toSubscription(SubscriptionEntity entity) {
        return new Subscription(entity.getId(), entity.getVariantKey(), entity.getGeneSymbol(), entity.getNtfyTopic());
    }
}
