package com.adamastorx.workers.clinvar;

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
 * VCF fixture into a bgzipped + tabix-indexed pair, the same file shape
 * ingestion actually deals with. Deliberately not shipped in {@code
 * src/main} -- production never needs to *compress* a plain VCF (NCBI
 * always delivers one already bgzipped); this only exists so tests can
 * start from a small, diffable, real-data-derived text fixture rather
 * than checking in an opaque binary {@code .vcf.gz}/{@code .tbi} pair.
 */
final class ClinVarFixtureSupport {

    private ClinVarFixtureSupport() {}

    /** Writes {@code clinvar.vcf.gz} + {@code clinvar.vcf.gz.tbi} into {@code destDir} from a plain-text fixture. */
    static void bgzipAndIndex(String classpathFixture, Path destDir) throws IOException {
        Files.createDirectories(destDir);
        Path vcfGz = destDir.resolve(ClinVarRefdataPaths.VCF_FILENAME);

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
