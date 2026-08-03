package com.adamastorx.aggregator.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backlog #81's own AC: "state store recovery measured for real". This is
 * the metric that measurement is built on, in production as well as in
 * {@code StateStoreRecoveryTest} (this module's test suite) -- a real
 * {@link StateRestoreListener} registered on the app's own {@code
 * KafkaStreams} instance, not a synthetic stand-in. Records {@code
 * aggregator_state_restore_duration_seconds} (a real Micrometer {@link
 * Timer}, one sample per (store, partition) restoration, tagged by store
 * name) on every restore this process ever does -- after a crash/
 * reschedule (this Deployment mounts no PVC for the state directory, see
 * platform's own deployment.yaml comment, so every real restart restores
 * fully from the changelog, not partially from local disk) or on first
 * startup against an already-populated changelog.
 *
 * <p>Also logs each restoration's start/end at INFO -- this is the
 * human-readable trail a real `kubectl logs` after a real pod kill would
 * show, the live-verification equivalent {@code
 * StateStoreRecoveryTest}'s own javadoc explains this metric stands in
 * for where a live cluster isn't available.
 */
public class StateRestoreMetrics implements StateRestoreListener {

    private static final Logger log = LoggerFactory.getLogger(StateRestoreMetrics.class);

    private final MeterRegistry meterRegistry;
    private final Map<String, Instant> restoreStartedAt = new ConcurrentHashMap<>();

    public StateRestoreMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    private static String key(TopicPartition topicPartition, String storeName) {
        return storeName + "|" + topicPartition;
    }

    @Override
    public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        restoreStartedAt.put(key(topicPartition, storeName), Instant.now());
        log.info(
                "State restore starting: store={} partition={} startingOffset={} endingOffset={} "
                        + "(records to replay={})",
                storeName,
                topicPartition,
                startingOffset,
                endingOffset,
                Math.max(0, endingOffset - startingOffset));
    }

    @Override
    public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        // No per-batch metric -- onRestoreEnd's total duration is the real
        // AC ask ("how long does restoration actually take"); a per-batch
        // gauge would be real data with no stated consumer, gold-plating
        // for v1 (ADR 0021).
    }

    @Override
    public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        Instant startedAt = restoreStartedAt.remove(key(topicPartition, storeName));
        if (startedAt == null) {
            // Defensive: should not happen (onRestoreStart always precedes
            // onRestoreEnd for the same partition/store per Kafka Streams'
            // own contract), but a missing start time must not throw out
            // of this listener -- an exception here would break the
            // restore path itself.
            log.warn(
                    "onRestoreEnd for store={} partition={} with no matching onRestoreStart -- "
                            + "duration not recorded",
                    storeName,
                    topicPartition);
            return;
        }
        Duration elapsed = Duration.between(startedAt, Instant.now());
        Timer.builder("aggregator_state_restore_duration_seconds")
                .description("Real, measured Kafka Streams state-store restoration time from the changelog "
                        + "topic (backlog #81's AC: 'state store recovery measured for real')")
                .tag("store", storeName)
                .publishPercentileHistogram()
                .register(meterRegistry)
                .record(elapsed);
        log.info(
                "State restore finished: store={} partition={} recordsRestored={} elapsed={}ms",
                storeName,
                topicPartition,
                totalRestored,
                elapsed.toMillis());
    }
}
