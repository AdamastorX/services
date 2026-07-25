package com.adamastorx.api.clinvar;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code clinvar.ingestion.completed} (services#26, ADR 0018) --
 * same manual-ack shape as {@code workers.workitem.WorkItemListener}.
 */
@Component
class ClinVarCacheInvalidationListener {

    private final VariantInvalidationService invalidationService;

    ClinVarCacheInvalidationListener(VariantInvalidationService invalidationService) {
        this.invalidationService = invalidationService;
    }

    @KafkaListener(
            topics = "${app.kafka.clinvar-ingestion-topic}",
            containerFactory = "clinVarIngestionEventKafkaListenerContainerFactory")
    void onMessage(ClinVarIngestionCompletedEvent event, Acknowledgment acknowledgment) {
        invalidationService.handleIngestionCompleted(event);
        acknowledgment.acknowledge();
    }
}
