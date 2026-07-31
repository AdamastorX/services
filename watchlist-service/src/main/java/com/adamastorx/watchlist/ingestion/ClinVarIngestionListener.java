package com.adamastorx.watchlist.ingestion;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Backlog #53's second, independent consumer of {@code
 * clinvar.ingestion.completed} -- api's {@code ClinVarCacheInvalidationListener}
 * (Redis-key eviction) is the first and only other one; this is a distinct
 * consumer group (application.yml), reads the same topic, has no
 * interaction with that consumer at all.
 */
@Component
class ClinVarIngestionListener {

    private final DeliveryResolutionService resolutionService;

    ClinVarIngestionListener(DeliveryResolutionService resolutionService) {
        this.resolutionService = resolutionService;
    }

    @KafkaListener(
            topics = "${app.kafka.clinvar-ingestion-topic}",
            containerFactory = "clinVarIngestionEventKafkaListenerContainerFactory")
    void onMessage(ClinVarIngestionCompletedEvent event, Acknowledgment acknowledgment) {
        resolutionService.resolveAndPersist(event);
        // Acknowledge only after the transactional insert above has committed --
        // see DeliveryResolutionService's javadoc for why this ordering is the
        // actual mechanism behind "a crash doesn't lose the notification".
        acknowledgment.acknowledge();
    }
}
