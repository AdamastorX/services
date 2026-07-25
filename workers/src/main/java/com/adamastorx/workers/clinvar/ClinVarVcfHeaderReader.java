package com.adamastorx.workers.clinvar;

import htsjdk.samtools.util.BlockCompressedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Parses the ClinVar VCF's own {@code ##fileDate} header line into a
 * {@link LocalDate} (services#25, ADR 0018 -- explicit AC: {@code
 * clinvar_release.published_date} must come from this header, not the
 * downloaded file's mtime, since mtime reflects when this app happened to
 * fetch it, not when NCBI actually cut the release).
 *
 * <p>Uses htsjdk's {@link BlockCompressedInputStream} rather than
 * {@code java.util.zip.GZIPInputStream} deliberately: a BGZF file is a
 * sequence of independently-compressed gzip members concatenated
 * together, and the plain JDK reader only decompresses the first member
 * before reporting EOF. The header happens to always fit in ClinVar's
 * first BGZF block in practice, but relying on that would be a latent bug
 * waiting for NCBI to reformat their header -- {@code
 * BlockCompressedInputStream} correctly walks the whole block sequence as
 * one continuous stream.
 */
final class ClinVarVcfHeaderReader {

    private static final String FILE_DATE_PREFIX = "##fileDate=";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private ClinVarVcfHeaderReader() {}

    static LocalDate readPublishedDate(Path bgzippedVcf) throws IOException {
        try (var blockStream = new BlockCompressedInputStream(bgzippedVcf.toFile());
                var reader = new BufferedReader(new InputStreamReader(blockStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(FILE_DATE_PREFIX)) {
                    return LocalDate.parse(line.substring(FILE_DATE_PREFIX.length()).trim(), DATE_FORMAT);
                }
                if (!line.startsWith("#")) {
                    // Past the header block entirely without finding it.
                    break;
                }
            }
        }
        throw new IOException("No ##fileDate header line found in " + bgzippedVcf);
    }
}
