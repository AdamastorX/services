package com.adamastorx.api.clinvar;

import java.net.URI;
import java.util.Optional;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriBuilder;

/**
 * HTTP client for {@code clinvar-service}'s internal lookup endpoint (ADR
 * 0019). Same pattern {@code gateway.ApiForwardingController} already
 * uses for {@code gateway} -&gt; {@code api} (ADR 0010) -- a plain
 * blocking Spring {@link RestClient} (already on the classpath via
 * spring-boot-starter-webmvc, no new dependency), built once from a base
 * URL sourced from an env var, applied here to a new internal boundary
 * ({@code api} -&gt; {@code clinvar-service}) instead of invented fresh.
 *
 * <p>This replaces {@code api}'s previous direct htsjdk/tabix file reads
 * and direct JPA access to {@code clinvar_release}/{@code
 * clinvar_variant_index} (both removed under ADR 0019 -- that data now
 * lives exclusively in {@code clinvar-service}'s own dedicated Postgres
 * instance, in its own namespace; {@code api} has no business reading
 * either directly, the exact class of cross-namespace mistake ADR 0019
 * exists to stop repeating).
 */
@Component
class ClinVarServiceClient {

    private final RestClient restClient;

    ClinVarServiceClient(@Value("${clinvar-service.base-url}") String clinVarServiceBaseUrl) {
        this.restClient = RestClient.builder().baseUrl(clinVarServiceBaseUrl).build();
    }

    /** {@code GET /internal/clinvar/lookup?chrom=...&pos=...&ref=...&alt=...} */
    Optional<ClinVarLookupResponse> lookupByCoordinates(String chrom, int pos, String ref, String alt) {
        return lookup(uriBuilder -> uriBuilder
                .path("/internal/clinvar/lookup")
                .queryParam("chrom", chrom)
                .queryParam("pos", pos)
                .queryParam("ref", ref)
                .queryParam("alt", alt)
                .build());
    }

    /** {@code GET /internal/clinvar/lookup?rsid=...} */
    Optional<ClinVarLookupResponse> lookupByRsid(String rsid) {
        return lookup(uriBuilder ->
                uriBuilder.path("/internal/clinvar/lookup").queryParam("rsid", rsid).build());
    }

    private Optional<ClinVarLookupResponse> lookup(Function<UriBuilder, URI> uriFunction) {
        try {
            ClinVarLookupResponse response =
                    restClient.get().uri(uriFunction).retrieve().body(ClinVarLookupResponse.class);
            return Optional.ofNullable(response);
        } catch (HttpClientErrorException.NotFound notFound) {
            return Optional.empty();
        } catch (RestClientException unreachable) {
            // clinvar-service is the source of truth here, not a cache --
            // unlike VariantAnnotationCacheService's Redis fail-open
            // pattern (ADR 0016), there is no local fallback data to serve
            // if it's unreachable, so this surfaces as a clear 502 rather
            // than a generic 500 or a silently empty result.
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "clinvar-service unavailable", unreachable);
        }
    }
}
