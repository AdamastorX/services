package com.adamastorx.api.clinvar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Release-aware cache invalidation (services#26, ADR 0018; drastically
 * simplified under ADR 0019). Previously this re-read both the old and
 * new release's tabix files itself to compute which cached keys' ClinVar
 * classification had changed -- {@code api} has no tabix/Postgres access
 * of any kind anymore, and doesn't need it for this: {@code
 * clinvar-service} already holds both releases locally and computes the
 * diff once, publishing the exact Redis keys to delete as {@code
 * changedKeys} on the {@code clinvar.ingestion.completed} event. This
 * service's whole job now is deleting each of those keys and counting
 * the eviction -- no scanning, no diffing, no VCF reads.
 */
@Service
public class VariantInvalidationService {

    private static final Logger log = LoggerFactory.getLogger(VariantInvalidationService.class);

    private final RedisTemplate<String, VariantAnnotation> redisTemplate;
    private final Counter invalidations;

    public VariantInvalidationService(
            RedisTemplate<String, VariantAnnotation> variantAnnotationRedisTemplate, MeterRegistry meterRegistry) {
        this.redisTemplate = variantAnnotationRedisTemplate;
        // Distinct from cache.gets{result=hit|miss|error} (ADR 0016) --
        // an invalidation-triggered eviction is neither a get nor a put,
        // it needs its own independently observable signal (ADR 0018 AC,
        // unchanged by ADR 0019).
        this.invalidations = meterRegistry.counter(
                "cache.invalidations", "cache", "variant-annotation", "reason", "release-changed");
    }

    void handleIngestionCompleted(ClinVarIngestionCompletedEvent event) {
        List<String> changedKeys = event.changedKeys();
        if (changedKeys == null || changedKeys.isEmpty()) {
            log.info(
                    "ClinVar release {} (previous {}): no changed keys to invalidate",
                    event.newReleaseId(),
                    event.previousReleaseId());
            return;
        }

        int evicted = 0;
        for (String key : changedKeys) {
            try {
                redisTemplate.delete(key);
                invalidations.increment();
                evicted++;
            } catch (DataAccessException ex) {
                // A Redis outage mid-sweep must not abort the rest of the
                // batch -- every other key in changedKeys still gets a
                // chance to be deleted (same fail-open spirit as
                // VariantAnnotationCacheService's get/put, ADR 0016).
                log.warn("Redis unavailable deleting invalidated key {} -- skipping it", key, ex);
            }
        }

        log.info(
                "ClinVar release {} (previous {}): evicted {} of {} changed keys",
                event.newReleaseId(),
                event.previousReleaseId(),
                evicted,
                changedKeys.size());
    }
}
