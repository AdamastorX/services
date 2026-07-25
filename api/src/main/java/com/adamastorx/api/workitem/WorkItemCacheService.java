package com.adamastorx.api.workitem;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Cache-aside for {@code GET /work-items/{id}} (services#5, ADR 0016).
 * Cache-aside, not write-through: {@link WorkItemController} never writes
 * here from {@code POST /work-items} -- a cache entry only gets created by
 * {@link #put} after a real read misses and falls through to PostgreSQL,
 * the textbook definition of the pattern this issue names.
 *
 * <p>Hand-rolled rather than Spring's declarative {@code @Cacheable} for
 * two concrete reasons (ADR 0016 has the full comparison): (1) Boot's
 * built-in cache-metrics binder has no Redis support, so
 * {@code @Cacheable} + {@code RedisCacheManager} alone implements the
 * caching but not the AC's "hit/miss ratio is an observable metric"
 * requirement without hand-writing a binder anyway; (2) the AC's other
 * hard requirement -- a Redis outage fails the read open to PostgreSQL,
 * not the request -- needs a custom {@code CacheErrorHandler} bean with
 * the declarative approach, less direct to reason about and test than a
 * plain try/catch here.
 *
 * <p>{@link WorkItemController} calls {@link #get} first; a miss and a
 * Redis error both come back as {@code Optional.empty()} and are handled
 * identically -- read PostgreSQL, then best-effort {@link #put} to fill
 * the cache. Deliberately no negative caching: a not-found id is never
 * written to Redis (ADR 0016 -- no abuse vector to defend against on a
 * ClusterIP-only, single-consumer cache, so the added complexity isn't
 * worth it here).
 */
@Service
public class WorkItemCacheService {

    private static final Logger log = LoggerFactory.getLogger(WorkItemCacheService.class);

    private static final String CACHE_NAME = "work-items";

    private final RedisTemplate<String, WorkItem> redisTemplate;
    private final Duration ttl;
    private final Counter hits;
    private final Counter misses;
    private final Counter getErrors;
    private final Counter putErrors;

    public WorkItemCacheService(
            RedisTemplate<String, WorkItem> workItemRedisTemplate,
            @Value("${app.cache.work-items.ttl:PT5M}") Duration ttl,
            MeterRegistry meterRegistry) {
        this.redisTemplate = workItemRedisTemplate;
        this.ttl = ttl;
        // Metric name/tags mirror Micrometer's own CacheMeterBinder
        // convention (cache.gets{cache=...,result=hit|miss}, cache.puts)
        // on purpose -- hand-rolled (see class javadoc), but a future
        // Grafana panel (backlog #20) built against the standard Spring
        // cache-metrics shape should work here unmodified.
        this.hits = meterRegistry.counter("cache.gets", "cache", CACHE_NAME, "result", "hit");
        this.misses = meterRegistry.counter("cache.gets", "cache", CACHE_NAME, "result", "miss");
        // A separate "error" result, not folded into "miss" -- an outage-
        // driven fallback isn't a real cache miss against a healthy cache;
        // folding it into "miss" would quietly pollute the hit-ratio metric
        // the hypothesis in ADR 0016 is meant to check against reality.
        this.getErrors = meterRegistry.counter("cache.gets", "cache", CACHE_NAME, "result", "error");
        this.putErrors = meterRegistry.counter("cache.puts", "cache", CACHE_NAME, "result", "error");
    }

    Optional<WorkItem> get(UUID id) {
        String key = key(id);
        try {
            WorkItem cached = redisTemplate.opsForValue().get(key);
            if (cached != null) {
                hits.increment();
                return Optional.of(cached);
            }
            misses.increment();
        } catch (DataAccessException ex) {
            // Fail open (services#5 AC): PostgreSQL, not this exception,
            // decides whether the request succeeds. WorkItemController
            // falls through to the repository on an empty Optional exactly
            // the same way it does for a plain miss -- the caller can't
            // tell the difference, by design.
            log.warn("Redis unavailable reading {} -- falling through to PostgreSQL", key, ex);
            getErrors.increment();
        }
        return Optional.empty();
    }

    void put(UUID id, WorkItem workItem) {
        String key = key(id);
        try {
            redisTemplate.opsForValue().set(key, workItem, ttl);
        } catch (DataAccessException ex) {
            // Best-effort: failing to populate the cache must never fail a
            // request that already has its answer from PostgreSQL.
            log.warn("Redis unavailable writing {} -- continuing without caching it", key, ex);
            putErrors.increment();
        }
    }

    private static String key(UUID id) {
        return CACHE_NAME + ":" + id;
    }
}
