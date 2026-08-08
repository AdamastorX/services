package com.adamastorx.marketdata.observability;

import java.util.concurrent.atomic.AtomicReference;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Backlog #86(b)/(c): a real recurrence of #86's own original incident,
 * this time with the real stack trace #86(a)'s logging fix made visible
 * (the original occurrence's own root cause was permanently lost to a
 * bare {@code .toString()} log). Root cause, confirmed live, not
 * theorized: a transient {@code ConfigException} during {@code
 * org.apache.kafka.clients.admin.AdminClientConfig}'s static initializer
 * (triggered indirectly by {@code KafkaTemplate}'s own observation/tracing
 * code, which resolves the cluster ID for the Kafka producer span) leaves
 * that class permanently unusable for the rest of the JVM's life -- per
 * the JVM spec, once a class's {@code <clinit>} throws, every later
 * reference to it throws {@link LinkageError} (concretely {@link
 * NoClassDefFoundError} here), forever, no self-heal possible short of a
 * restart. The pod stayed {@code 1/1 Running}/{@code Healthy} the entire
 * time -- the same silent-but-Healthy shape #85(b) already fixed for
 * {@code aggregator}'s Kafka Streams client, now given the same real
 * answer for this module's plain Kafka producer.
 *
 * <p><b>The decision: gate LIVENESS on a detected {@link LinkageError}
 * from a real publish attempt, leave READINESS untouched.</b> A {@link
 * LinkageError} is not a transient, retriable Kafka condition (a broker
 * timeout, a temporary unavailability) -- those throw {@code
 * org.apache.kafka.common.errors.*} checked exceptions, never a {@link
 * LinkageError}, so this check cannot misfire on ordinary, recoverable
 * broker trouble and force an unnecessary restart. It specifically
 * targets the one real failure mode that is genuinely unrecoverable
 * without a restart, the same precision {@code
 * KafkaStreamsLivenessHealthIndicator} applies to Kafka Streams'
 * own real terminal {@code ERROR} state rather than gating on any
 * non-{@code RUNNING} state.
 *
 * <p>Once tripped, stays down permanently (by design, matching the real
 * JVM semantics this class exists to surface) -- there is no real
 * "recovered" state for a poisoned class short of the restart this
 * indicator's own {@code DOWN} report is meant to trigger.
 */
@Component("kafkaProducer")
public class KafkaProducerLivenessHealthIndicator implements HealthIndicator {

    private final AtomicReference<String> poisonedBy = new AtomicReference<>();

    /**
     * Called from {@link com.adamastorx.marketdata.tick.StockPriceTickPublisher}
     * on every real publish failure. Only a {@link LinkageError} anywhere
     * in the cause chain trips liveness -- everything else (a broker
     * timeout, a transient network issue) is a real, ordinary, recoverable
     * publish failure this indicator deliberately ignores.
     */
    public void recordPublishFailure(Throwable ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof LinkageError) {
                poisonedBy.compareAndSet(null, cause.getClass().getName() + ": " + cause.getMessage());
                return;
            }
            cause = cause.getCause();
        }
    }

    @Override
    public Health health() {
        String detail = poisonedBy.get();
        if (detail != null) {
            return Health.down()
                    .withDetail("reason", "Kafka producer permanently poisoned by a LinkageError (backlog #86) -- "
                            + "requires a pod restart, cannot self-heal")
                    .withDetail("cause", detail)
                    .build();
        }
        return Health.up().build();
    }
}
