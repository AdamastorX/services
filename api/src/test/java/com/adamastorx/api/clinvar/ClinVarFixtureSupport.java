package com.adamastorx.api.clinvar;

import htsjdk.samtools.util.BlockCompressedOutputStream;
import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.variant.vcf.VCFCodec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test-only support for turning a small, human-readable, checked-in plain
 * VCF fixture into a bgzipped + tabix-indexed pair -- same helper as
 * {@code workers.clinvar.ClinVarFixtureSupport} (duplicated per-module
 * test code, same reasoning as the rest of this domain's duplication:
 * see {@code ClinVarRelease}'s javadoc). Production code in this module
 * never compresses a VCF itself (it only ever reads one {@code workers}
 * already downloaded pre-bgzipped) -- this exists purely so integration
 * tests can start from a small, diffable, real-data-derived text fixture.
 */
final class ClinVarFixtureSupport {

    private ClinVarFixtureSupport() {}

    static void bgzipAndIndex(String classpathFixture, Path destDir) throws IOException {
        Files.createDirectories(destDir);
        Path vcfGz = destDir.resolve("clinvar.vcf.gz");

        try (InputStream in = ClinVarFixtureSupport.class.getResourceAsStream("/" + classpathFixture);
                BlockCompressedOutputStream out = new BlockCompressedOutputStream(vcfGz.toFile())) {
            if (in == null) {
                throw new IOException("Fixture not found on classpath: " + classpathFixture);
            }
            in.transferTo(out);
        }

        VCFCodec codec = new VCFCodec();
        var index = IndexFactory.createTabixIndex(vcfGz, codec, TabixFormat.VCF, null);
        index.write(Path.of(vcfGz + ".tbi"));
    }
}
