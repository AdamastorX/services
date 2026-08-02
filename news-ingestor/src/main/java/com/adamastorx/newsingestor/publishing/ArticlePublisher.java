package com.adamastorx.newsingestor.publishing;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes one {@code ArticlePublishedEvent} to {@code
 * news.article.published}, keyed on the first matched ticker (a
 * reasonable partition key for "recent news per ticker" locality; unlike
 * {@code work-items}, this topic has an obvious domain key to use).
 *
 * <p>Bounded, synchronous send with an explicit timeout -- same
 * reasoning as {@code api}'s {@code OutboxRelay} and {@code
 * watchlist-service}'s {@code NotificationRelay}: a bare {@code get()}
 * with no timeout can block this service's single scheduled poll thread
 * for up to Kafka's own default {@code delivery.timeout.ms} (2 minutes)
 * during a real broker outage. Unlike the outbox pattern, a failed send
 * here is dropped, not retried from a persisted PENDING row -- this
 * service is deliberately stateless (see {@code ArticleDedupService}'s
 * javadoc and the README); a Kafka outage during a poll cycle means that
 * cycle's matched articles are lost rather than replayed, an accepted v1
 * gap the README states explicitly rather than glosses over. The article
 * is still marked seen in the dedup cache regardless of publish outcome,
 * so a later successful poll of the same still-current feed item won't
 * retry it either -- if this gap matters in practice, the fix is the same
 * outbox pattern {@code api}/{@code watchlist-service} already use, not
 * invented fresh here.
 */
@Component
public class ArticlePublisher {

    private static final Logger log = LoggerFactory.getLogger(ArticlePublisher.class);

    private final KafkaTemplate<String, ArticlePublishedEvent> kafkaTemplate;
    private final String topic;
    private final long sendTimeoutMs;

    public ArticlePublisher(
            KafkaTemplate<String, ArticlePublishedEvent> kafkaTemplate,
            @Value("${app.kafka.news-article-published-topic}") String topic,
            @Value("${app.kafka.send-timeout-ms:5000}") long sendTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    /** @return true if the send completed successfully. */
    public boolean publish(ArticlePublishedEvent event) {
        try {
            String key = event.tickers().isEmpty() ? null : event.tickers().get(0);
            kafkaTemplate.send(topic, key, event).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception ex) {
            log.warn("Failed to publish {} for article {} ({}): {}", topic, event.guid(), event.link(), ex.getMessage());
            return false;
        }
    }
}
