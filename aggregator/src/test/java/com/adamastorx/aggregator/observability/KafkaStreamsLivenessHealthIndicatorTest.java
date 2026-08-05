package com.adamastorx.aggregator.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.topology.AggregatorTopology;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Backlog #85(b): proves {@link KafkaStreamsLivenessHealthIndicator}'s
 * decision rule for real, at two different levels of strength, stated
 * plainly rather than left implicit:
 *
 * <ul>
 *   <li><b>{@code RUNNING} and {@code ERROR} -- the strong, preferred
 *   proof.</b> A real embedded KRaft broker ({@link
 *   EmbeddedKafkaKraftBroker}, the same real process {@code
 *   StateStoreRecoveryTest} uses, not {@code TopologyTestDriver} and not a
 *   mock) and a real {@link KafkaStreams} instance for each: one runs the
 *   real production topology ({@link AggregatorTopology#build}) to a real
 *   {@code RUNNING} state; the other runs a small, deliberately-throwing
 *   topology with a real {@code streamsUncaughtExceptionHandler} set to
 *   {@code SHUTDOWN_CLIENT} -- the exact real reaction backlog #85's own
 *   incident hit -- fed one real record to trigger it, and waited for a
 *   real, live-broker-induced {@code PENDING_ERROR -> ERROR} transition.
 *   Nothing about either {@code KafkaStreams} instance's own behavior is
 *   faked; the indicator's {@link KafkaStreamsLivenessHealthIndicator#health(KafkaStreams.State)}
 *   rule is exercised directly against each instance's own real {@code
 *   .state()} read afterward.</li>
 *   <li><b>{@code CREATED}/{@code REBALANCING}/{@code PENDING_SHUTDOWN}/
 *   {@code NOT_RUNNING}/{@code null} -- the real, stated minimum.</b>
 *   Exercised directly against the real {@code
 *   org.apache.kafka.streams.KafkaStreams.State} enum constants
 *   (confirmed against the actual {@code kafka-streams:4.2.1} dependency
 *   jar, not assumed from memory) rather than against a live instance
 *   sitting in each state. Reliably forcing a real, running instance to
 *   sit in a transient state like {@code REBALANCING} without flakiness
 *   would need real multi-instance rebalance choreography -- out of
 *   proportion to what this indicator's own deliberately simple
 *   classification (only {@code ERROR} is {@code DOWN}) needs proven, and
 *   this project's own StateStoreRecoveryTest already independently
 *   establishes the real ~45s window a restart genuinely spends in
 *   exactly these transient states. This is not a mock standing in for a
 *   live broker: the indicator's own logic runs against the real enum
 *   values Kafka Streams itself defines, nothing about {@code
 *   KafkaStreams}' own behavior is simulated.</li>
 * </ul>
 */
class KafkaStreamsLivenessHealthIndicatorTest {

    private EmbeddedKafkaBroker broker;
    private KafkaStreams streams;
    private Path stateDir;

    @AfterEach
    void tearDown() throws IOException {
        if (streams != null) {
            streams.close(Duration.ofSeconds(15));
        }
        if (broker != null) {
            broker.destroy();
        }
        deleteRecursively(stateDir);
    }

    @Test
    void reportsUpForEveryRealTransientOrNotYetStartedStateARestartGenuinelyPassesThrough() {
        // Real enum constants from the actual kafka-streams:4.2.1 jar --
        // see this class's own javadoc for why these five are exercised
        // this way rather than against a live instance forced into each
        // one.
        assertThat(KafkaStreamsLivenessHealthIndicator.health(KafkaStreams.State.CREATED).getStatus())
                .isEqualTo(Status.UP);
        assertThat(KafkaStreamsLivenessHealthIndicator.health(KafkaStreams.State.REBALANCING).getStatus())
                .isEqualTo(Status.UP);
        assertThat(KafkaStreamsLivenessHealthIndicator.health(KafkaStreams.State.PENDING_SHUTDOWN).getStatus())
                .isEqualTo(Status.UP);
        assertThat(KafkaStreamsLivenessHealthIndicator.health(KafkaStreams.State.NOT_RUNNING).getStatus())
                .isEqualTo(Status.UP);
        // No KafkaStreams instance yet (very early startup) -- not the
        // same real failure ERROR is, and this app's liveness probe
        // already has its own initialDelaySeconds to cover ordinary cold
        // start.
        assertThat(KafkaStreamsLivenessHealthIndicator.health(null).getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void reportsUpAgainstARealKafkaStreamsInstanceThatActuallyReachedRunning() throws Exception {
        broker = new EmbeddedKafkaKraftBroker(1, 1, "stock.price.tick", "news.sentiment.scored");
        broker.afterPropertiesSet();

        AggregatorProperties properties = new AggregatorProperties(
                "stock.price.tick",
                "news.sentiment.scored",
                Duration.ofMinutes(15),
                Duration.ZERO,
                "price-window-store",
                "sentiment-window-store",
                List.of("AAPL"));

        StreamsBuilder builder = new StreamsBuilder();
        AggregatorTopology.build(builder, properties);
        Topology topology = builder.build();

        String applicationId = "aggregator-liveness-test-running-" + UUID.randomUUID();
        stateDir = Files.createTempDirectory("aggregator-liveness-running-");
        streams = new KafkaStreams(topology, streamsProps(applicationId, broker.getBrokersAsString(), stateDir));
        streams.start();
        waitForState(streams, KafkaStreams.State.RUNNING, Duration.ofSeconds(30));

        // The real state this real instance actually reached -- not
        // assumed.
        assertThat(streams.state()).isEqualTo(KafkaStreams.State.RUNNING);

        Health health = KafkaStreamsLivenessHealthIndicator.health(streams.state());
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("kafkaStreams.state", "RUNNING");
    }

    @Test
    void reportsDownAgainstARealKafkaStreamsInstanceForcedIntoARealErrorStateByAnActualCrash() throws Exception {
        String inputTopic = "kafka-streams-liveness-test-crash-input";
        broker = new EmbeddedKafkaKraftBroker(1, 1, inputTopic);
        broker.afterPropertiesSet();

        // A minimal, deliberately-throwing topology -- not the production
        // AggregatorTopology (RocksDB/BlockBasedTableConfig-shaped
        // failures, backlog #85's own two real root causes, are already
        // fixed and out of scope here per this backlog item's own AC; this
        // test only needs to force the real, generic
        // PENDING_ERROR -> ERROR transition every fatal Kafka Streams
        // failure funnels through, the exact real terminal state this
        // indicator gates on).
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(org.apache.kafka.common.serialization.Serdes.String(),
                        org.apache.kafka.common.serialization.Serdes.String()))
                .foreach((key, value) -> {
                    throw new RuntimeException(
                            "Deliberate test failure -- forcing a real KafkaStreams.State.ERROR transition, "
                                    + "the same real terminal state backlog #85's own real incident hit "
                                    + "(SHUTDOWN_CLIENT).");
                });
        Topology topology = builder.build();

        String applicationId = "aggregator-liveness-test-error-" + UUID.randomUUID();
        stateDir = Files.createTempDirectory("aggregator-liveness-error-");
        streams = new KafkaStreams(topology, streamsProps(applicationId, broker.getBrokersAsString(), stateDir));
        // The exact real reaction backlog #85's own incident hit -- see
        // KafkaStreamsLivenessHealthIndicator's own javadoc.
        streams.setUncaughtExceptionHandler(
                exception -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT);
        streams.start();
        waitForState(streams, KafkaStreams.State.RUNNING, Duration.ofSeconds(30));

        produceOneRecord(broker.getBrokersAsString(), inputTopic);

        // The real, live-broker-induced terminal state -- not asserted
        // from memory or from Kafka Streams' own Javadoc alone.
        waitForState(streams, KafkaStreams.State.ERROR, Duration.ofSeconds(30));
        assertThat(streams.state()).isEqualTo(KafkaStreams.State.ERROR);

        Health health = KafkaStreamsLivenessHealthIndicator.health(streams.state());
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("kafkaStreams.state", "ERROR");
    }

    private void produceOneRecord(String bootstrapServers, String topic) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>(topic, "AAPL", "trigger")).get();
        }
    }

    private Properties streamsProps(String applicationId, String bootstrapServers, Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    private void waitForState(KafkaStreams streams, KafkaStreams.State target, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (streams.state() != target && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        assertThat(streams.state()).isEqualTo(target);
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // best-effort test cleanup
                }
            });
        }
    }
}
