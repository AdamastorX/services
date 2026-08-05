package com.adamastorx.marketdata.finnhub;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link FinnhubQuote} actually deserializes Finnhub's real,
 * documented REST {@code /quote} response shape -- mirrors {@code
 * FinnhubMessageParsingTest}'s own "the parsing contract is unit-tested in
 * isolation" precedent for the websocket's wire shape.
 */
class FinnhubQuoteParsingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesARealQuoteResponse() throws Exception {
        String json =
                """
                {"c":213.14,"d":1.25,"dp":0.59,"h":214.0,"l":211.5,"o":212.0,"pc":211.89,"t":1690000000}
                """;

        FinnhubQuote quote = objectMapper.readValue(json, FinnhubQuote.class);

        assertThat(quote.currentPrice()).isEqualByComparingTo(new BigDecimal("213.14"));
        assertThat(quote.epochSeconds()).isEqualTo(1690000000L);
    }

    @Test
    void ignoresUnknownFieldsRatherThanFailing() throws Exception {
        // A conservative parsing contract: an unexpected field Finnhub adds
        // in the future must not break deserialization of the two fields
        // this service actually uses.
        String json = """
                {"c":1.0,"t":1,"aNewFieldFinnhubAddsLater":"x"}
                """;

        FinnhubQuote quote = objectMapper.readValue(json, FinnhubQuote.class);

        assertThat(quote.currentPrice()).isEqualByComparingTo(BigDecimal.ONE);
        assertThat(quote.epochSeconds()).isEqualTo(1L);
    }

    @Test
    void parsesTheNoQuoteAvailableShape() throws Exception {
        // Finnhub's own documented behavior for a symbol it has no real
        // quote for right now: every field 0, not an HTTP error --
        // FinnhubQuotePoller treats this the same as a failed poll (see
        // its own javadoc), never as a real $0.00 price.
        String json = """
                {"c":0,"d":0,"dp":0,"h":0,"l":0,"o":0,"pc":0,"t":0}
                """;

        FinnhubQuote quote = objectMapper.readValue(json, FinnhubQuote.class);

        assertThat(quote.currentPrice()).isEqualByComparingTo(BigDecimal.ZERO);
    }
}
