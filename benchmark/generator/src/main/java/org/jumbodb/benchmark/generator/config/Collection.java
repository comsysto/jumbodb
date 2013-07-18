package org.jumbodb.benchmark.generator.config;

public class Collection {

    private String name;
    private String description;
    private String strategy;
    private int numberOfFiles;
    private int dataSetSizeInByte;
    private int dataSetsPerFile;
    private int snappyBlockSizeInByte;

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

    public int getDataSetSizeInByte() {
        return dataSetSizeInByte;
    }

    public void setDataSetSizeInByte(int dataSetSizeInByte) {
        this.dataSetSizeInByte = dataSetSizeInByte;
    }

    public int getDataSetsPerFile() {
        return dataSetsPerFile;
    }

    public void setDataSetsPerFile(int dataSetsPerFile) {
        this.dataSetsPerFile = dataSetsPerFile;
    }

    public int getSnappyBlockSizeInByte() {
        return snappyBlockSizeInByte;
    }

    public void setSnappyBlockSizeInByte(int snappyBlockSizeInByte) {
        this.snappyBlockSizeInByte = snappyBlockSizeInByte;
    }
}
