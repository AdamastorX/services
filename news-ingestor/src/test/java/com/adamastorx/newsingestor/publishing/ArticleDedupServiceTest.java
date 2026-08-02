package com.adamastorx.newsingestor.publishing;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ArticleDedupServiceTest {

    @Test
    void firstSightingIsNewSecondIsNot() {
        ArticleDedupService dedup = new ArticleDedupService(2000);

        assertThat(dedup.markIfNew("guid-1")).isTrue();
        assertThat(dedup.markIfNew("guid-1")).isFalse();
        assertThat(dedup.markIfNew("guid-1")).isFalse();
    }

    @Test
    void differentKeysAreIndependentlyNew() {
        ArticleDedupService dedup = new ArticleDedupService(2000);

        assertThat(dedup.markIfNew("guid-1")).isTrue();
        assertThat(dedup.markIfNew("guid-2")).isTrue();
    }

    @Test
    void boundedCacheEvictsOldestOnceCapExceeded() {
        ArticleDedupService dedup = new ArticleDedupService(2);

        assertThat(dedup.markIfNew("guid-1")).isTrue();
        assertThat(dedup.markIfNew("guid-2")).isTrue();
        assertThat(dedup.markIfNew("guid-3")).isTrue(); // evicts guid-1

        // guid-1 was evicted, so it looks new again -- an accepted, bounded
        // v1 tradeoff (see class javadoc), not a bug: the cap only needs to
        // comfortably exceed either feed's real current item count.
        assertThat(dedup.markIfNew("guid-1")).isTrue();
        // guid-3 is still within the cap window.
        assertThat(dedup.markIfNew("guid-3")).isFalse();
    }

    @Test
    void blankOrNullKeyIsTreatedAsAlwaysNewRatherThanPermanentlyDuplicate() {
        ArticleDedupService dedup = new ArticleDedupService(2000);

        assertThat(dedup.markIfNew(null)).isTrue();
        assertThat(dedup.markIfNew("")).isTrue();
        assertThat(dedup.markIfNew(null)).isTrue();
    }
}
