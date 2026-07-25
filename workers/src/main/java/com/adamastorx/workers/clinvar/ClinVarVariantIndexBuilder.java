package com.adamastorx.workers.clinvar;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Populates {@code clinvar_variant_index} during ingestion (services#25,
 * ADR 0018) and returns the total variant count read from the file (which
 * becomes {@code clinvar_release.variant_count}).
 *
 * <p>Plain {@link JdbcTemplate} batch inserts rather than
 * {@code JpaRepository.saveAll} deliberately: this is a bulk ETL load of
 * up to a few million rows per ingestion, not request-path CRUD --
 * per-entity JPA persistence-context tracking for that volume is
 * unnecessary overhead this class has no reason to pay. Nothing else in
 * this module needs a JPA entity for this table at all (only {@code api}
 * reads it back, via its own {@code ClinVarVariantIndexEntity}/repository
 * pair), so one wasn't added here.
 *
 * <p>Only variants that carry an {@code RS} (dbSNP rs-number) INFO value
 * are indexed -- this table exists specifically to make rsID lookups
 * possible (ADR 0018); a variant with no rsID has nothing to be indexed
 * *by* and is only ever reachable via the coordinate-based lookup path,
 * which queries the tabix file directly and never touches this table.
 */
@Component
class ClinVarVariantIndexBuilder {

    private static final Logger log = LoggerFactory.getLogger(ClinVarVariantIndexBuilder.class);

    private static final int BATCH_SIZE = 1_000;

    private static final String INSERT_SQL =
            "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?)";

    private static final String DELETE_OTHER_RELEASES_SQL =
            "DELETE FROM clinvar_variant_index WHERE clinvar_release_id <> ?";

    private final JdbcTemplate jdbcTemplate;

    ClinVarVariantIndexBuilder(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Streams {@code bgzippedVcf} once, indexing every rsID-bearing
     * variant against {@code releaseId}. Returns the total number of VCF
     * records seen (indexed or not) -- this is {@code
     * clinvar_release.variant_count}, "a variant count matching the
     * source file" per the issue's AC, not just the count of rows
     * actually indexed.
     */
    long build(Path bgzippedVcf, UUID releaseId) {
        long totalRecords = 0;
        List<IndexRow> batch = new ArrayList<>(BATCH_SIZE);

        try (VCFFileReader reader = new VCFFileReader(bgzippedVcf.toFile(), false)) {
            for (VariantContext context : reader) {
                totalRecords++;
                List<String> rsIds = context.getAttributeAsStringList("RS", null);
                if (rsIds.isEmpty()) {
                    continue;
                }
                String chrom = context.getContig();
                int pos = context.getStart();
                String ref = context.getReference().getDisplayString();
                for (Allele alt : context.getAlternateAlleles()) {
                    String altBases = alt.getDisplayString();
                    for (String rsId : rsIds) {
                        batch.add(new IndexRow(normalizeRsId(rsId), chrom, pos, ref, altBases, releaseId));
                        if (batch.size() >= BATCH_SIZE) {
                            flush(batch);
                        }
                    }
                }
            }
        }
        flush(batch);

        log.info("Indexed variant lookup rows for release {} ({} total VCF records scanned)", releaseId, totalRecords);
        return totalRecords;
    }

    /** Deletes index rows for every release other than {@code keepReleaseId} (see class javadoc). */
    void pruneOtherReleases(UUID keepReleaseId) {
        int deleted = jdbcTemplate.update(DELETE_OTHER_RELEASES_SQL, keepReleaseId);
        log.info("Pruned {} clinvar_variant_index rows from non-current releases", deleted);
    }

    private void flush(List<IndexRow> batch) {
        if (batch.isEmpty()) {
            return;
        }
        List<IndexRow> toFlush = List.copyOf(batch);
        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                IndexRow row = toFlush.get(i);
                ps.setString(1, row.rsid());
                ps.setString(2, row.chrom());
                ps.setInt(3, row.pos());
                ps.setString(4, row.ref());
                ps.setString(5, row.alt());
                ps.setObject(6, row.releaseId());
            }

            @Override
            public int getBatchSize() {
                return toFlush.size();
            }
        });
        batch.clear();
    }

    /** ClinVar's own {@code RS} INFO values are bare numbers (e.g. {@code 80357906}), not {@code rs}-prefixed. */
    private static String normalizeRsId(String rawRsId) {
        return "rs" + rawRsId.replaceFirst("(?i)^rs", "");
    }

    private record IndexRow(String rsid, String chrom, int pos, String ref, String alt, UUID releaseId) {}
}
