package org.jumbodb.benchmark.generator.config;

public class Collection {

    private String name;
    private String description;
    private String strategy;
    private int numberOfFiles;
    private int datasetSizeInByte;
    private long datasetsPerFile;
    private long snappyBlockSizeInByte;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public int getNumberOfFiles() {
        return numberOfFiles;
    }

    public void setNumberOfFiles(int numberOfFiles) {
        this.numberOfFiles = numberOfFiles;
    }

    public int getDatasetSizeInByte() {
        return datasetSizeInByte;
    }

    public void setDatasetSizeInByte(int datasetSizeInByte) {
        this.datasetSizeInByte = datasetSizeInByte;
    }

    public long getDatasetsPerFile() {
        return datasetsPerFile;
    }

    public void setDatasetsPerFile(long datasetsPerFile) {
        this.datasetsPerFile = datasetsPerFile;
    }

    public long getSnappyBlockSizeInByte() {
        return snappyBlockSizeInByte;
    }

    public void setSnappyBlockSizeInByte(long snappyBlockSizeInByte) {
        this.snappyBlockSizeInByte = snappyBlockSizeInByte;
    }
}
