package org.jumbodb.benchmark.suite.config;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class DataCollectionRun {
    private String strategy;
    private List<Integer> numberOfThreads;
    private List<Integer> datasetLoadGroupSize;
    private int numberOfSamplesPerRun;

    public List<Integer> getNumberOfThreads() {
        return numberOfThreads;
    }

    public void setNumberOfThreads(List<Integer> numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public List<Integer> getDatasetLoadGroupSize() {
        return datasetLoadGroupSize;
    }

    public void setDatasetLoadGroupSize(List<Integer> datasetLoadGroupSize) {
        this.datasetLoadGroupSize = datasetLoadGroupSize;
    }

    public int getNumberOfSamplesPerRun() {
        return numberOfSamplesPerRun;
    }

    public void setNumberOfSamplesPerRun(int numberOfSamplesPerRun) {
        this.numberOfSamplesPerRun = numberOfSamplesPerRun;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    @Override
    public String toString() {
        return "DataCollectionRun{" +
                "strategy='" + strategy + '\'' +
                ", numberOfThreads=" + numberOfThreads +
                ", datasetLoadGroupSize=" + datasetLoadGroupSize +
                ", numberOfSamplesPerRun=" + numberOfSamplesPerRun +
                '}';
    }
}
