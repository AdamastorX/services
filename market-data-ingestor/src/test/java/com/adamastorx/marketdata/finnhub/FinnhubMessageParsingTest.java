package com.adamastorx.marketdata.finnhub;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * Proves {@link FinnhubMessage}/{@link FinnhubTrade} actually deserialize
 * Finnhub's real, documented wire shapes (ADR 0029's research) --
 * {@link FinnhubWebSocketClient#handleMessage} is otherwise only
 * exercisable against a real live connection (see that class's own
 * javadoc and {@code admin.ReconnectController}'s), so this is the one
 * place the parsing contract itself is unit-tested in isolation.
 */
class FinnhubMessageParsingTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesARealTradeMessage() throws Exception {
        String json =
                """
                {"data":[{"c":["1","12"],"p":123.45,"s":"AAPL","t":1690000000000,"v":10}],"type":"trade"}
                """;

        FinnhubMessage message = objectMapper.readValue(json, FinnhubMessage.class);

        assertThat(message.type()).isEqualTo("trade");
        assertThat(message.data()).hasSize(1);
        FinnhubTrade trade = message.data().get(0);
        assertThat(trade.symbol()).isEqualTo("AAPL");
        assertThat(trade.price()).isEqualByComparingTo(new BigDecimal("123.45"));
        assertThat(trade.volume()).isEqualByComparingTo(new BigDecimal("10"));
        assertThat(trade.epochMillis()).isEqualTo(1690000000000L);
    }

    @Test
    void parsesAPingMessageWithNoDataField() throws Exception {
        String json = """
                {"type":"ping"}
                """;

        FinnhubMessage message = objectMapper.readValue(json, FinnhubMessage.class);

        assertThat(message.type()).isEqualTo("ping");
        assertThat(message.data()).isNull();
    }

    @Test
    void parsesAnErrorMessage() throws Exception {
        String json =
                """
                {"type":"error","msg":"Symbol not supported"}
                """;

        FinnhubMessage message = objectMapper.readValue(json, FinnhubMessage.class);

        assertThat(message.type()).isEqualTo("error");
        assertThat(message.msg()).isEqualTo("Symbol not supported");
    }

    @Test
    void ignoresUnknownFieldsRatherThanFailing() throws Exception {
        // A conservative parsing contract: an unexpected field Finnhub adds
        // in the future (e.g. a new trade-condition shape) must not break
        // deserialization of the fields this service actually uses.
        String json =
                """
                {"data":[{"p":1.0,"s":"AAPL","t":1,"v":1,"aNewFieldFinnhubAddsLater":"x"}],"type":"trade","aNewTopLevelField":42}
                """;

        FinnhubMessage message = objectMapper.readValue(json, FinnhubMessage.class);

        assertThat(message.type()).isEqualTo("trade");
        assertThat(message.data().get(0).symbol()).isEqualTo("AAPL");
    }
}
