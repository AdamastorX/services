package com.adamastorx.newsingestor.feed;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Parses the {@code <item>} elements of an RSS 2.0 feed via JDK StAX
 * ({@code javax.xml.stream}) -- no new parsing dependency (e.g. Rome):
 * both chosen feeds are plain RSS 2.0 with a handful of fields this
 * project actually needs (guid/title/description/link/pubDate), and StAX
 * ships in the JDK, matching this project's "boring, well-understood
 * tools" bias (ADR 0029/0021) over a library that would mostly go unused.
 */
@Component
public class RssFeedParser {

    private static final Logger log = LoggerFactory.getLogger(RssFeedParser.class);

    // RSS 2.0's pubDate is RFC 822 (e.g. "Sun, 02 Aug 2026 18:50:26 GMT"),
    // exactly what DateTimeFormatter.RFC_1123_DATE_TIME parses -- confirmed
    // against a live fetch of both chosen feeds, not assumed from the spec.
    private static final DateTimeFormatter PUB_DATE_FORMAT = DateTimeFormatter.RFC_1123_DATE_TIME;

    private static final XMLInputFactory XML_INPUT_FACTORY = newHardenedFactory();

    private static XMLInputFactory newHardenedFactory() {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        // These feeds are untrusted external input over the network --
        // disable DTD/external-entity resolution (XXE) even though neither
        // feed currently uses one, same defensive-by-default posture the
        // rest of this project applies to anything crossing a trust
        // boundary.
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        return factory;
    }

    /**
     * @param source a short, stable label ("wsj-markets", "marketwatch")
     *     copied onto every {@link RssArticle} this feed produces --
     *     becomes {@code news.article.published}'s {@code source} field.
     */
    public List<RssArticle> parse(String xml, String source) {
        List<RssArticle> articles = new ArrayList<>();
        InputStream input = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        try {
            XMLStreamReader reader = XML_INPUT_FACTORY.createXMLStreamReader(input);
            try {
                while (reader.hasNext()) {
                    int event = reader.next();
                    if (event == XMLStreamConstants.START_ELEMENT && "item".equals(reader.getLocalName())) {
                        articles.add(parseItem(reader, source));
                    }
                }
            } finally {
                reader.close();
            }
        } catch (XMLStreamException ex) {
            // A malformed feed body is the same shape as an unreachable feed
            // from FeedPoller's point of view: log and skip this poll cycle,
            // don't crash the scheduler thread. Whatever items were parsed
            // before the malformed point are still returned/used -- most
            // feeds fail late (a truncated response), not on item 1.
            log.warn("Failed to parse RSS feed from source {}: {}", source, ex.getMessage());
        }
        return articles;
    }

    private RssArticle parseItem(XMLStreamReader reader, String source) throws XMLStreamException {
        String guid = null;
        String link = null;
        String title = null;
        String description = null;
        String pubDateRaw = null;

        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.END_ELEMENT && "item".equals(reader.getLocalName())) {
                break;
            }
            if (event == XMLStreamConstants.START_ELEMENT) {
                String name = reader.getLocalName();
                switch (name) {
                    case "guid" -> guid = reader.getElementText();
                    case "link" -> link = reader.getElementText();
                    case "title" -> title = reader.getElementText();
                    case "description" -> description = reader.getElementText();
                    case "pubDate" -> pubDateRaw = reader.getElementText();
                    default -> { /* ignore media:content, dc:creator, etc. -- not needed */ }
                }
            }
        }

        return new RssArticle(
                guid,
                link,
                title == null ? "" : title.trim(),
                description == null ? "" : description.trim(),
                parsePubDate(pubDateRaw, source),
                source);
    }

    private Instant parsePubDate(String pubDateRaw, String source) {
        if (pubDateRaw == null || pubDateRaw.isBlank()) {
            return Instant.now();
        }
        try {
            return java.time.ZonedDateTime.parse(pubDateRaw.trim(), PUB_DATE_FORMAT).toInstant();
        } catch (DateTimeParseException ex) {
            // One item with an unparsable date shouldn't drop the whole
            // article -- fall back to "now" and keep going, same
            // skip-the-bad-part-not-the-whole-cycle spirit as the
            // feed-level catch above.
            log.warn("Failed to parse pubDate '{}' from source {}, defaulting to now: {}", pubDateRaw, source, ex.getMessage());
            return Instant.now();
        }
    }
}
