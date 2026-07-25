package com.adamastorx.api.clinvar;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

/**
 * JPA row for {@code clinvar_variant_index} (services#24/#25, ADR 0018)
 * -- read-only from {@code api}'s side; {@code workers} populates this
 * table in bulk via plain JDBC (see {@code
 * workers.clinvar.ClinVarVariantIndexBuilder}'s javadoc for why no JPA
 * entity exists on that side at all). This is the only module that needs
 * one, for {@link ClinVarVariantIndexRepository#findByRsid}.
 */
@Entity
@Table(name = "clinvar_variant_index")
public class ClinVarVariantIndexEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String rsid;

    @Column(nullable = false)
    private String chrom;

    @Column(nullable = false)
    private int pos;

    @Column(nullable = false)
    private String ref;

    @Column(nullable = false)
    private String alt;

    @Column(name = "clinvar_release_id", nullable = false)
    private UUID clinvarReleaseId;

    protected ClinVarVariantIndexEntity() {
        // JPA
    }

    public String getRsid() {
        return rsid;
    }

    public String getChrom() {
        return chrom;
    }

    public int getPos() {
        return pos;
    }

    public String getRef() {
        return ref;
    }

    public String getAlt() {
        return alt;
    }

    public UUID getClinvarReleaseId() {
        return clinvarReleaseId;
    }
}
