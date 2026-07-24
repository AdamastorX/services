package com.adamastorx.api.workitem;

/**
 * Trivial message payload published to the {@code work-items} topic (ADR
 * 0011). Deliberately not extracted into {@code shared}: ADR 0007 calls
 * out DTOs specifically as something not to move there speculatively, and
 * producer/consumer are kept decoupled from each other's compiled Java
 * types on purpose — see {@code workers.workitem.WorkItem} and the wire
 * contract note in {@code workers/README.md}. This is a scaffold/proof of
 * the Kafka path, not a real domain object.
 */
public record WorkItem(String id, String message) {
}
