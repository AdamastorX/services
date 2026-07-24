package com.adamastorx.workers.workitem;

/**
 * Trivial message payload consumed from the {@code work-items} topic (ADR
 * 0011). Kept as workers' own copy rather than a {@code shared}-module
 * type: producer ({@code api.workitem.WorkItem}) and consumer are
 * decoupled from each other's compiled Java class on the wire — the
 * producer disables Kafka JSON type headers
 * ({@code spring.json.add.type.headers: false}) and this side deserializes
 * into a fixed default type
 * ({@code spring.json.value.default.type}), so the two modules only need
 * to agree on the JSON shape, not share a class. This mirrors how a real
 * cross-service Kafka contract would work (a schema, not a shared jar) and
 * keeps ADR 0007's "not speculatively for DTOs" guidance intact.
 */
public record WorkItem(String id, String message) {
}
