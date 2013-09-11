package org.jumbodb.benchmark.suite.config;

import java.util.List;

public class DataCollection {

    private String collectionName;
    private String chunkKey;
    private String description;
    private String offsetGenerator;
    private List<DataCollectionRun> runs;

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public void setChunkKey(String chunkKey) {
        this.chunkKey = chunkKey;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOffsetGenerator() {
        return offsetGenerator;
    }

    public void setOffsetGenerator(String offsetGenerator) {
        this.offsetGenerator = offsetGenerator;
    }

    public List<DataCollectionRun> getRuns() {
        return runs;
    }

    public void setRuns(List<DataCollectionRun> runs) {
        this.runs = runs;
    }
}
