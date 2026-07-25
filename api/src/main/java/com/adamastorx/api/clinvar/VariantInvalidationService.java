package com.adamastorx.api.clinvar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;

/**
 * Release-aware cache invalidation (services#26, ADR 0018) -- the
 * project's first invalidation-on-write cache behaviour, distinct from
 * every existing TTL-only cache story. On a completed ingestion: {@code
 * SCAN} only the keys actually cached under {@code variantAnnotation:*}
 * (never a full diff of ClinVar's ~2M+ records), point-query just those
 * coordinates against both the old and new release's tabix files, and
 * {@code DEL} only the entries whose classification actually changed.
 */
@Service
public class VariantInvalidationService {

    private static final Logger log = LoggerFactory.getLogger(VariantInvalidationService.class);

    private final RedisTemplate<String, VariantAnnotation> redisTemplate;
    private final ClinVarVcfQueryService vcfQueryService;
    private final ClinVarRefdataPaths paths;
    private final Counter invalidations;

    public VariantInvalidationService(
            RedisTemplate<String, VariantAnnotation> variantAnnotationRedisTemplate,
            ClinVarVcfQueryService vcfQueryService,
            ClinVarRefdataPaths paths,
            MeterRegistry meterRegistry) {
        this.redisTemplate = variantAnnotationRedisTemplate;
        this.vcfQueryService = vcfQueryService;
        this.paths = paths;
        // Distinct from cache.gets{result=hit|miss|error} (ADR 0016) --
        // an invalidation-triggered eviction is neither a get nor a put,
        // it needs its own independently observable signal (ADR 0018 AC).
        this.invalidations = meterRegistry.counter(
                "cache.invalidations", "cache", "variant-annotation", "reason", "release-changed");
    }

    void handleIngestionCompleted(ClinVarIngestionCompletedEvent event) {
        if (event.previousReleaseId() == null) {
            log.info(
                    "ClinVar release {} has no previous release to diff against -- nothing to invalidate",
                    event.releaseId());
            return;
        }

        UUID newReleaseId = UUID.fromString(event.releaseId());
        UUID previousReleaseId = UUID.fromString(event.previousReleaseId());
        Path newVcf = paths.releaseVcfPath(newReleaseId);
        Path oldVcf = paths.releaseVcfPath(previousReleaseId);

        Set<String> cachedKeys = scanCachedKeys();
        int evicted = 0;
        for (String key : cachedKeys) {
            try {
                if (classificationChanged(key, oldVcf, newVcf)) {
                    redisTemplate.delete(key);
                    invalidations.increment();
                    evicted++;
                }
            } catch (RuntimeException ex) {
                // One malformed/unparseable key or a transient htsjdk
                // failure must not abort the whole sweep -- every other
                // cached key still gets a chance to be checked.
                log.warn("Failed to evaluate cached key {} for invalidation -- skipping it", key, ex);
            }
        }

        log.info(
                "ClinVar release {} (previous {}): scanned {} cached variant-annotation keys, evicted {}",
                newReleaseId,
                previousReleaseId,
                cachedKeys.size(),
                evicted);
    }

    private boolean classificationChanged(String key, Path oldVcf, Path newVcf) {
        VariantCoordinateKey coordinates = VariantCoordinateKey.parse(key);
        if (coordinates == null) {
            return false;
        }
        Optional<ClinVarVcfQueryService.VcfHit> before = vcfQueryService.query(
                oldVcf, coordinates.chrom(), coordinates.pos(), coordinates.ref(), coordinates.alt());
        Optional<ClinVarVcfQueryService.VcfHit> after = vcfQueryService.query(
                newVcf, coordinates.chrom(), coordinates.pos(), coordinates.ref(), coordinates.alt());

        String beforeSignificance = before.map(ClinVarVcfQueryService.VcfHit::clinicalSignificance).orElse(null);
        String afterSignificance = after.map(ClinVarVcfQueryService.VcfHit::clinicalSignificance).orElse(null);
        return !java.util.Objects.equals(beforeSignificance, afterSignificance);
    }

    /**
     * {@code SCAN}, not {@code KEYS} -- the whole point of this method
     * (ADR 0018 AC: bound the cost to what's actually cached, not a
     * blocking full-keyspace command). Operates on the raw {@link
     * RedisTemplate#execute} connection since {@code SCAN} works on raw
     * keys, independent of the template's configured serializers.
     */
    private Set<String> scanCachedKeys() {
        Set<String> keys = new HashSet<>();
        try {
            redisTemplate.execute((org.springframework.data.redis.connection.RedisConnection connection) -> {
                try (Cursor<byte[]> cursor = connection.keyCommands()
                        .scan(ScanOptions.scanOptions()
                                .match(VariantAnnotationCacheService.REDIS_KEY_PREFIX + ":*")
                                .count(200)
                                .build())) {
                    while (cursor.hasNext()) {
                        keys.add(new String(cursor.next(), StandardCharsets.UTF_8));
                    }
                }
                return null;
            });
        } catch (DataAccessException ex) {
            log.warn("Redis unavailable while scanning {}:* for invalidation -- skipping this sweep entirely",
                    VariantAnnotationCacheService.REDIS_KEY_PREFIX, ex);
        }
        return keys;
    }

    /** Parses a {@code variantAnnotation:{chrom}:{pos}:{ref}:{alt}} cache key back into its parts. */
    private record VariantCoordinateKey(String chrom, int pos, String ref, String alt) {

        static VariantCoordinateKey parse(String key) {
            String[] parts = key.split(":");
            if (parts.length != 5) {
                return null;
            }
            try {
                return new VariantCoordinateKey(parts[1], Integer.parseInt(parts[2]), parts[3], parts[4]);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }
}
