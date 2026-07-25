package com.adamastorx.api.clinvar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Cache-aside for variant lookups (services#24, ADR 0018) -- exact same
 * hand-rolled, fail-open pattern as {@code workitem.WorkItemCacheService}
 * (services#15, ADR 0016), reused deliberately rather than reinvented: a
 * Redis outage falls back to the tabix lookup the same way a work-item
 * read falls back to PostgreSQL, and the two "result" counters
 * (hit/miss/error) share the exact {@code cache.gets} metric name with a
 * different {@code cache} tag value ({@code "variant-annotation"} instead
 * of {@code "work-items"}) specifically so existing Grafana
 * queries/dashboards (backlog #20) generalize across both without
 * per-cache-specific panels.
 *
 * <p><strong>Redis key prefix vs. metric tag, deliberately different
 * strings</strong>: the metric {@code cache} tag is {@code
 * "variant-annotation"} (kebab-case, matching {@code WorkItemCacheService}'s
 * {@code "work-items"} convention), but the actual Redis key prefix is
 * {@code variantAnnotation} (camelCase) -- services#26's AC names this
 * exact keyspace ({@code SCAN variantAnnotation:*}) for its
 * cache-invalidation consumer, so that literal prefix is preserved here
 * rather than "fixed" to match the metric tag's casing.
 *
 * <p>Unlike {@code WorkItemCacheService}, this cache is <em>not</em>
 * TTL-only in practice -- services#26 adds active invalidation-on-write
 * when a new ClinVar release changes a cached variant's classification.
 * The TTL below is still a real, independent memory/key-count hygiene
 * bound (same role it plays for work-items), not the correctness
 * mechanism; correctness against a reclassification comes from
 * invalidation, not expiry.
 */
@Service
public class VariantAnnotationCacheService {

    private static final Logger log = LoggerFactory.getLogger(VariantAnnotationCacheService.class);

    private static final String METRIC_CACHE_NAME = "variant-annotation";
    static final String REDIS_KEY_PREFIX = "variantAnnotation";

    private final RedisTemplate<String, VariantAnnotation> redisTemplate;
    private final Duration ttl;
    private final Counter hits;
    private final Counter misses;
    private final Counter getErrors;
    private final Counter putErrors;

    public VariantAnnotationCacheService(
            RedisTemplate<String, VariantAnnotation> variantAnnotationRedisTemplate,
            @Value("${app.cache.variant-annotation.ttl:PT30M}") Duration ttl,
            MeterRegistry meterRegistry) {
        this.redisTemplate = variantAnnotationRedisTemplate;
        this.ttl = ttl;
        this.hits = meterRegistry.counter("cache.gets", "cache", METRIC_CACHE_NAME, "result", "hit");
        this.misses = meterRegistry.counter("cache.gets", "cache", METRIC_CACHE_NAME, "result", "miss");
        this.getErrors = meterRegistry.counter("cache.gets", "cache", METRIC_CACHE_NAME, "result", "error");
        this.putErrors = meterRegistry.counter("cache.puts", "cache", METRIC_CACHE_NAME, "result", "error");
    }

    Optional<VariantAnnotation> get(String chrom, int pos, String ref, String alt) {
        String key = key(chrom, pos, ref, alt);
        try {
            VariantAnnotation cached = redisTemplate.opsForValue().get(key);
            if (cached != null) {
                hits.increment();
                return Optional.of(cached);
            }
            misses.increment();
        } catch (DataAccessException ex) {
            log.warn("Redis unavailable reading {} -- falling through to the tabix lookup", key, ex);
            getErrors.increment();
        }
        return Optional.empty();
    }

    void put(String chrom, int pos, String ref, String alt, VariantAnnotation annotation) {
        String key = key(chrom, pos, ref, alt);
        try {
            redisTemplate.opsForValue().set(key, annotation, ttl);
        } catch (DataAccessException ex) {
            log.warn("Redis unavailable writing {} -- continuing without caching it", key, ex);
            putErrors.increment();
        }
    }

    static String key(String chrom, int pos, String ref, String alt) {
        return REDIS_KEY_PREFIX + ":" + chrom + ":" + pos + ":" + ref + ":" + alt;
    }
}
