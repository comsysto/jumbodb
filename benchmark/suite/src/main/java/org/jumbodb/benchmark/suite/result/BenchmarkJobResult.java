package org.jumbodb.benchmark.suite.result;

/**
 * @author Carsten Hufe
 */
public class BenchmarkJobResult {
    private String collection;
    private int numberOfInputThreads;
    private int datasetLoadGroupSize;
    private int numberOfSamplesPerRun;
    private long minResponseMs;
    private long maxResponseMs;
    private long averageResponseMs;
    private long averageDataRateBytesPerSecond;

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public int getNumberOfInputThreads() {
        return numberOfInputThreads;
    }

    public void setNumberOfInputThreads(int numberOfInputThreads) {
        this.numberOfInputThreads = numberOfInputThreads;
    }

    public int getDatasetLoadGroupSize() {
        return datasetLoadGroupSize;
    }

    public void setDatasetLoadGroupSize(int datasetLoadGroupSize) {
        this.datasetLoadGroupSize = datasetLoadGroupSize;
    }

    public int getNumberOfSamplesPerRun() {
        return numberOfSamplesPerRun;
    }

    public void setNumberOfSamplesPerRun(int numberOfSamplesPerRun) {
        this.numberOfSamplesPerRun = numberOfSamplesPerRun;
    }

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

    @Override
    public String toString() {
        return "BenchmarkJobResult{" +
                "collection='" + collection + '\'' +
                ", numberOfInputThreads=" + numberOfInputThreads +
                ", datasetLoadGroupSize=" + datasetLoadGroupSize +
                ", numberOfSamplesPerRun=" + numberOfSamplesPerRun +
                ", minResponseMs=" + minResponseMs +
                ", maxResponseMs=" + maxResponseMs +
                ", averageResponseMs=" + averageResponseMs +
                ", averageDataRateBytesPerSecond=" + averageDataRateBytesPerSecond +
                '}';
    }
}
