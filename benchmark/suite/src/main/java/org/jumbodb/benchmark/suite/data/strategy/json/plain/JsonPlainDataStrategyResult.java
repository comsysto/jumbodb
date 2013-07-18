package org.jumbodb.benchmark.suite.data.strategy.json.plain;

/**
 * @author Carsten Hufe
 */
public class JsonPlainDataStrategyResult {
    private long minResponseMs;
    private long maxResponseMs;
    private long averageResponseMs;
    private long averageDataRateBytesPerSecond;
    private long numberOfReadActions;
    private long fullExecutionTimeInMs;

    public long getMinResponseMs() {
        return minResponseMs;
    }

    public void setMinResponseMs(long minResponseMs) {
        this.minResponseMs = minResponseMs;
    }

    public long getMaxResponseMs() {
        return maxResponseMs;
    }

    public void setMaxResponseMs(long maxResponseMs) {
        this.maxResponseMs = maxResponseMs;
    }

    public long getAverageResponseMs() {
        return averageResponseMs;
    }

    public void setAverageResponseMs(long averageResponseMs) {
        this.averageResponseMs = averageResponseMs;
    }

    public long getAverageDataRateBytesPerSecond() {
        return averageDataRateBytesPerSecond;
    }

    public void setAverageDataRateBytesPerSecond(long averageDataRateBytesPerSecond) {
        this.averageDataRateBytesPerSecond = averageDataRateBytesPerSecond;
    }

    public long getNumberOfReadActions() {
        return numberOfReadActions;
    }

    public void setNumberOfReadActions(long numberOfReadActions) {
        this.numberOfReadActions = numberOfReadActions;
    }

    public long getFullExecutionTimeInMs() {
        return fullExecutionTimeInMs;
    }

    public void setFullExecutionTimeInMs(long fullExecutionTimeInMs) {
        this.fullExecutionTimeInMs = fullExecutionTimeInMs;
    }
}
