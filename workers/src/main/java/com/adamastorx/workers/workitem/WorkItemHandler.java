package com.adamastorx.workers.workitem;

/**
 * Processing seam for a consumed {@link WorkItem}, kept separate from
 * {@link WorkItemListener} so tests can swap in a capturing implementation
 * without needing a live Kafka broker to observe what got processed.
 */
public interface WorkItemHandler {

    void handle(WorkItem workItem);
}
