package com.adamastorx.marketdata.observability;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.KafkaException;

/**
 * backlog #86(b)/(c): proves the real decision rule directly against real
 * exception types, not a mocked chain -- a real {@link
 * NoClassDefFoundError} (the exact class that poisoned this module's own
 * Kafka producer live) trips this indicator; a real, ordinary Kafka
 * exception a transient broker issue would actually throw does not, and
 * neither does one wrapped several layers deep the way a real
 * {@code KafkaTemplate} failure chain nests them.
 */
class KafkaProducerLivenessHealthIndicatorTest {

    @Test
    void startsUp() {
        var indicator = new KafkaProducerLivenessHealthIndicator();
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void aRealLinkageErrorTripsItDown() {
        var indicator = new KafkaProducerLivenessHealthIndicator();
        indicator.recordPublishFailure(new NoClassDefFoundError("Could not initialize class "
                + "org.apache.kafka.clients.admin.AdminClientConfig"));
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void aLinkageErrorNestedInsideARealKafkaExceptionStillTripsIt() {
        var indicator = new KafkaProducerLivenessHealthIndicator();
        var wrapped = new KafkaException("Failed to send", new ExceptionInInitializerError(
                new NoClassDefFoundError("Could not initialize class AdminClientConfig")));
        indicator.recordPublishFailure(wrapped);
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void anOrdinaryRecoverableKafkaFailureDoesNotTripIt() {
        var indicator = new KafkaProducerLivenessHealthIndicator();
        indicator.recordPublishFailure(
                new org.apache.kafka.common.errors.TimeoutException("Topic not present in metadata after 60000 ms."));
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void stayingDownIsPermanentOnceTripped() {
        var indicator = new KafkaProducerLivenessHealthIndicator();
        indicator.recordPublishFailure(new NoClassDefFoundError("poisoned"));
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
        // A later, unrelated ordinary failure must not "recover" it back
        // to UP -- there is no real recovered state for a poisoned class
        // short of the restart this DOWN report exists to trigger.
        indicator.recordPublishFailure(
                new org.apache.kafka.common.errors.TimeoutException("unrelated transient failure"));
        assertThat(indicator.health().getStatus()).isEqualTo(Status.DOWN);
    }
}
