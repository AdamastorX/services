package com.adamastorx.api.workitem;

import java.util.List;

/**
 * backlog #130: {@code GET /work-items}' real pagination envelope —
 * {@code items} plus enough metadata ({@code page}/{@code size}/
 * {@code totalElements}/{@code totalPages}) for a real caller to know
 * whether there's more to fetch, without exposing Spring Data's own
 * internal {@code Page} shape (its {@code Pageable}/sort fields) over
 * the wire.
 */
public record WorkItemPage(List<WorkItem> items, int page, int size, long totalElements, int totalPages) {
}
