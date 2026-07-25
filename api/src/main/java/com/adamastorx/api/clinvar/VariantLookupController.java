package com.adamastorx.api.clinvar;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * {@code GET /variants/lookup} (services#24, ADR 0018): accepts either
 * {@code (chrom, pos, ref, alt)} or {@code rsid}, mutually exclusive --
 * both or neither is a {@code 400} with a clear message, not a silently
 * ambiguous "pick one" default.
 */
@RestController
public class VariantLookupController {

    private final VariantLookupService lookupService;

    public VariantLookupController(VariantLookupService lookupService) {
        this.lookupService = lookupService;
    }

    @GetMapping("/variants/lookup")
    public ResponseEntity<VariantAnnotation> lookup(
            @RequestParam(required = false) String chrom,
            @RequestParam(required = false) Integer pos,
            @RequestParam(required = false) String ref,
            @RequestParam(required = false) String alt,
            @RequestParam(required = false) String rsid) {
        boolean byCoordinates = chrom != null || pos != null || ref != null || alt != null;
        boolean byRsid = rsid != null;

        if (byCoordinates && byRsid) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Provide either (chrom, pos, ref, alt) or rsid, not both");
        }
        if (!byCoordinates && !byRsid) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Provide either (chrom, pos, ref, alt) or rsid");
        }

        if (byRsid) {
            return lookupService
                    .lookupByRsid(rsid)
                    .map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build());
        }

        if (chrom == null || pos == null || ref == null || alt == null) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Coordinate lookup requires all of chrom, pos, ref, and alt");
        }
        return lookupService
                .lookupByCoordinates(chrom, pos, ref, alt)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
