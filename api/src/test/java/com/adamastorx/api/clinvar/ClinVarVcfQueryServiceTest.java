package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit-level (no Spring, no Postgres, no Redis, no Docker) proof that the
 * htsjdk point-lookup + INFO-field extraction actually works against a
 * real, bgzipped-and-tabix-indexed fixture -- the part of services#24
 * this sandbox can verify directly, without Testcontainers.
 */
class ClinVarVcfQueryServiceTest {

    @TempDir
    private Path refdataRoot;

    @Test
    void resolvesRealPathogenicBrca1AndBrca2Variants() throws Exception {
        Path currentDir = refdataRoot.resolve("current");
        ClinVarFixtureSupport.bgzipAndIndex("clinvar/fixture-release-1.vcf", currentDir);

        ClinVarRefdataPaths paths = new ClinVarRefdataPaths(refdataRoot.toString());
        ClinVarVcfQueryService queryService = new ClinVarVcfQueryService(paths);

        Optional<ClinVarVcfQueryService.VcfHit> brca1 = queryService.query("17", 43057062, "T", "TG");
        assertThat(brca1).isPresent();
        assertThat(brca1.get().clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(brca1.get().reviewStatus()).isEqualTo("reviewed_by_expert_panel");
        assertThat(brca1.get().rsid()).isEqualTo("rs80357906");

        Optional<ClinVarVcfQueryService.VcfHit> brca2 = queryService.query("13", 32340300, "GT", "G");
        assertThat(brca2).isPresent();
        assertThat(brca2.get().clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(brca2.get().rsid()).isEqualTo("rs80359550");
    }

    @Test
    void wrongAllelesAtAKnownPositionDoNotMatch() throws Exception {
        Path currentDir = refdataRoot.resolve("current");
        ClinVarFixtureSupport.bgzipAndIndex("clinvar/fixture-release-1.vcf", currentDir);

        ClinVarRefdataPaths paths = new ClinVarRefdataPaths(refdataRoot.toString());
        ClinVarVcfQueryService queryService = new ClinVarVcfQueryService(paths);

        // Same position as the real BRCA1 variant, but a REF/ALT combination
        // not present in the fixture -- must not spuriously match.
        assertThat(queryService.query("17", 43057062, "T", "C")).isEmpty();
        assertThat(queryService.query("1", 12345, "A", "G")).isEmpty();
    }
}
