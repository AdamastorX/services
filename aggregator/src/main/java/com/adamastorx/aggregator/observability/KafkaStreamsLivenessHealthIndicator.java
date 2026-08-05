package com.adamastorx.aggregator.observability;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/**
 * Backlog #85(b): the real, stated decision on the liveness gap backlog #85
 * found live and left open on purpose while (a) (the RocksDB/Alpine and
 * BlockBasedTableConfig incidents, {@code services#52}/{@code #53}) got
 * fixed. Both real incidents shared the exact same dangerous shape: the
 * Kafka Streams client hit a fatal, unrecoverable error, its own
 * uncaught-exception handler shut it down ({@code SHUTDOWN_CLIENT},
 * landing in the real, terminal {@link KafkaStreams.State#ERROR}), and the
 * pod's own liveness/readiness probes (Spring Boot's default health
 * groups, which know nothing about Kafka Streams' own internal state)
 * kept reporting {@code Healthy}/{@code Running} the entire time. Nothing
 * told Kubernetes to actually restart the pod -- only a human watching
 * real logs after each live sync caught it, twice.
 *
 * <p><b>The decision: gate LIVENESS on {@code ERROR}, leave READINESS
 * exactly as it is.</b> These are different questions with different
 * right answers here:
 *
 * <ul>
 *   <li><b>Readiness</b> ("should this pod receive traffic right now?")
 *   correctly stays ungated on Kafka Streams' state -- see {@code
 *   platform/kubernetes/aggregator/deployment.yaml}'s own comment and this
 *   module's README ("ADR 0011, resolved"): a real, measured ~45-second
 *   restore/consumer-group-rejoin window happens on *every* normal
 *   restart (this Deployment mounts no PVC for the state directory, so
 *   every restart forces a full changelog replay), and {@link
 *   com.adamastorx.aggregator.api.AggregateQueryService#isReady()}
 *   already gives a more honest, finer-grained signal (a real {@code 503}
 *   while restoring) than a readiness probe that would otherwise flap the
 *   pod in and out of the Service's endpoint list on every restart. That
 *   reasoning is real and unchanged by this class.</li>
 *   <li><b>Liveness</b> ("should Kubernetes kill and restart this
 *   container?") asks a genuinely different question, and {@code ERROR}
 *   is exactly the case a liveness probe exists to catch: per the real
 *   {@code org.apache.kafka.streams.KafkaStreams.State} enum (confirmed
 *   against the actual {@code kafka-streams:4.2.1} dependency jar and its
 *   own real Javadoc, not assumed from memory), {@code ERROR} is a
 *   terminal state reached only via {@code PENDING_ERROR -> ERROR} --
 *   "not recoverable, and only a restart would get an application back to
 *   the RUNNING state" (the enum's own real Javadoc). The transient
 *   states a normal restore/rebalance genuinely passes through on the way
 *   to {@code RUNNING} -- {@code CREATED}, {@code REBALANCING} -- must
 *   NOT flip liveness, or this would reintroduce exactly the flapping
 *   readiness was deliberately spared from, just on the liveness probe
 *   instead. {@code NOT_RUNNING} (reached via a graceful {@code
 *   PENDING_SHUTDOWN -> NOT_RUNNING}, i.e. this process's own {@code
 *   close()} being called, not a crash) is also left UP here: by the time
 *   that happens the container is already tearing itself down on purpose
 *   (this app never calls {@code close()} on its own streams instance
 *   outside of shutdown), so there is no real pod left for a liveness
 *   probe to usefully kill.</li>
 * </ul>
 *
 * <p><b>Real verification that this cannot reintroduce probe flapping.</b>
 * {@code platform/kubernetes/aggregator/deployment.yaml}'s liveness probe
 * allows {@code initialDelaySeconds(20) + periodSeconds(10) *
 * failureThreshold(6) = 80s} of real, continuous non-{@code ERROR} state
 * before failing the pod -- comfortably more than the ~45s real restore/
 * rejoin window {@code StateStoreRecoveryTest} measured (this module's
 * README), and {@code CREATED}/{@code REBALANCING} both report {@code UP}
 * here for that entire window, so a normal restart never trips this.
 *
 * <p>Registered under the bean name {@code "kafkaStreams"} (an explicit
 * {@link Component} name, not left to Spring Boot's default
 * suffix-stripping bean-naming convention) and wired into the {@code
 * liveness} health group via {@code
 * management.endpoint.health.group.liveness.include}
 * (aggregator's own {@code application.yml}) -- Boot's own {@code
 * AvailabilityProbesHealthEndpointGroups} (confirmed against the actual
 * {@code spring-boot-health:4.1.0} jar/source) only auto-creates the
 * {@code liveness}/{@code readiness} groups from {@code livenessState}/
 * {@code readinessState} alone when the user has NOT already defined a
 * group with that name; once {@code management.endpoint.health.group.
 * liveness.include} is set at all, it fully replaces that default
 * membership rather than adding to it -- which is why {@code
 * livenessState} is listed explicitly alongside {@code kafkaStreams}
 * there, not dropped.
 */
@Component("kafkaStreams")
public class KafkaStreamsLivenessHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean factoryBean;

    public KafkaStreamsLivenessHealthIndicator(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @Override
    public Health health() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        return health(streams == null ? null : streams.state());
    }

    /**
     * The real decision rule, factored out so it can be exercised directly
     * against the real {@link KafkaStreams.State} enum (including a
     * genuine, live-broker-induced {@code ERROR} transition) without
     * needing a full Spring context -- see {@code
     * KafkaStreamsLivenessHealthIndicatorTest}.
     *
     * <p>{@code null} (the factory bean exists but {@code
     * getKafkaStreams()} hasn't produced an instance yet, e.g. very early
     * in application startup) is treated as {@code UP} -- not yet started
     * is not the same real failure {@code ERROR} is, and this app's
     * liveness probe already has its own {@code initialDelaySeconds} to
     * cover ordinary cold start.
     */
    static Health health(KafkaStreams.State state) {
        if (state == KafkaStreams.State.ERROR) {
            return Health.down()
                    .withDetail("kafkaStreams.state", state.name())
                    .build();
        }
        return Health.up()
                .withDetail("kafkaStreams.state", state == null ? "NOT_STARTED" : state.name())
                .build();
    }
}
