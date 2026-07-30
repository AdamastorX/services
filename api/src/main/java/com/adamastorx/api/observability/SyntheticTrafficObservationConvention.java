package com.adamastorx.api.observability;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import org.springframework.http.server.observation.DefaultServerRequestObservationConvention;
import org.springframework.http.server.observation.ServerRequestObservationContext;
import org.springframework.stereotype.Component;

/**
 * backlog#45 AC: generated (synthetic) traffic must be distinguishable from
 * real manual traffic <em>at query time</em>, not just documented in a
 * comment somewhere -- every dashboard and fact pack built against {@code
 * http_server_requests_seconds_count}/{@code _sum} needs a real PromQL
 * label to filter or exclude the permanent workload generator's load, so
 * nothing ever silently attributes it to a real request.
 *
 * <p>Extends Boot 4's {@link DefaultServerRequestObservationConvention}
 * (the observation-based replacement for the old {@code
 * WebMvcTagsContributor}/{@code WebMvcTagsProvider} mechanism, which no
 * longer exists on this classpath -- spring-web's {@code
 * ServerHttpObservationFilter} produces {@code http.server.requests} via
 * this convention instead) rather than reimplementing it -- every existing
 * tag (method/uri/status/exception/outcome, which ADR 0020's SLO/alert
 * expressions already key off) stays exactly as-is; this only adds one
 * low-cardinality tag on top of what Boot already produces.
 *
 * <p>Registering a single bean of type {@code ServerRequestObservationConvention}
 * is Spring Boot's documented, supported extension point here --
 * {@code WebMvcObservationAutoConfiguration} looks up exactly one such bean
 * (via {@code ObjectProvider::getIfAvailable}) and uses it in place of the
 * default it would otherwise construct, so this class *is* the default
 * logic (via inheritance) plus the one addition, not a parallel
 * implementation that can drift from it on a future Boot upgrade.
 *
 * <p>{@code services/workload-generator} (backlog#45) is the one caller
 * expected to ever send {@link #SYNTHETIC_USER_AGENT_PREFIX} -- every other
 * client (a browser, a human's {@code curl}, {@code clinvar-viewer}) gets
 * {@code traffic_source="real"} by definition, since none of them has a
 * reason to send this exact string.
 */
@Component
public class SyntheticTrafficObservationConvention extends DefaultServerRequestObservationConvention {

    /**
     * Must match {@code USER_AGENT} in {@code
     * services/workload-generator/generator/client.py} exactly -- the two
     * are deliberately not shared via a common module (a Python script and
     * a Spring bean have no natural shared dependency here), so this
     * literal string is the contract between them, spelled out in both
     * places' javadoc/comments rather than assumed.
     */
    public static final String SYNTHETIC_USER_AGENT_PREFIX = "AdamastorX-WorkloadGenerator/";

    static final String TAG_NAME = "traffic_source";

    @Override
    public KeyValues getLowCardinalityKeyValues(ServerRequestObservationContext context) {
        return super.getLowCardinalityKeyValues(context).and(trafficSource(context));
    }

    private static KeyValue trafficSource(ServerRequestObservationContext context) {
        String userAgent = context.getCarrier().getHeader("User-Agent");
        boolean synthetic = userAgent != null && userAgent.startsWith(SYNTHETIC_USER_AGENT_PREFIX);
        return KeyValue.of(TAG_NAME, synthetic ? "synthetic" : "real");
    }
}
