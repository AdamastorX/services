package com.adamastorx.workers.workitem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Default {@link WorkItemHandler}: just logs. No business processing yet
 * (scaffold/proof, services#3) — real work lands in a future issue.
 */
@Component
public class LoggingWorkItemHandler implements WorkItemHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingWorkItemHandler.class);

    @Override
    public void handle(WorkItem workItem) {
        log.info("Consumed work item id={} message={}", workItem.id(), workItem.message());
    }
}
