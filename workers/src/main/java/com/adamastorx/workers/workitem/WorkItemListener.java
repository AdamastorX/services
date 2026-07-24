package com.adamastorx.workers.workitem;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes the {@code work-items} topic (ADR 0011). Consumer group id is
 * {@code spring.application.name} ({@code workers}), set via
 * {@code spring.kafka.consumer.group-id} in {@code application.yml} rather
 * than repeated here, so there is exactly one place that pins it.
 *
 * <p>Manual {@code AckMode.MANUAL_IMMEDIATE} ({@code enable.auto.commit:
 * false}, ack mode set explicitly in {@link WorkItemConsumerConfig}): the offset
 * only commits after {@link #onMessage} returns normally, so a crash
 * mid-processing (or a rebalance triggered by scaling replicas)
 * redelivers the record to whichever replica picks up the partition next,
 * instead of silently losing it — at-least-once delivery. See
 * {@code workers/README.md} for the consumer-group behaviour this
 * produces with 3 partitions and multiple replicas.
 */
@Component
public class WorkItemListener {

    private final WorkItemHandler handler;

    public WorkItemListener(WorkItemHandler handler) {
        this.handler = handler;
    }

    @KafkaListener(
            topics = "${app.kafka.work-items-topic}",
            containerFactory = "workItemKafkaListenerContainerFactory")
    public void onMessage(WorkItem workItem, Acknowledgment acknowledgment) {
        handler.handle(workItem);
        acknowledgment.acknowledge();
    }
}
