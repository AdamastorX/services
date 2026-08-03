package com.adamastorx.aggregator.topology;

/**
 * The running aggregate this topology's {@code sentiment-window-store}
 * keeps per (ticker, window): a sample count and score sum, from which
 * {@link #averageScore()} derives the rolling average sentiment backlog
 * #81's AC names. A sum+count pair, not a running average field directly
 * -- keeps {@code accumulate} a pure, order-independent fold (matches
 * Kafka Streams' own aggregation contract: an {@code Aggregator} must be
 * associative/commutative-safe under reprocessing, and computing a
 * running average incrementally from only the previous average plus one
 * new sample is a well-known source of subtle rounding bugs a sum/count
 * pair avoids entirely).
 */
public record SentimentWindowAggregate(long sampleCount, double scoreSum) {

    public static SentimentWindowAggregate empty() {
        return new SentimentWindowAggregate(0, 0.0);
    }

    public SentimentWindowAggregate accumulate(double score) {
        return new SentimentWindowAggregate(sampleCount + 1, scoreSum + score);
    }

    public double averageScore() {
        return sampleCount == 0 ? 0.0 : scoreSum / sampleCount;
    }
}
