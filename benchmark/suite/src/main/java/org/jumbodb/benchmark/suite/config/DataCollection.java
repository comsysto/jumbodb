package org.jumbodb.benchmark.suite.config;

import java.util.List;

public class DataCollection {

    private String name;
    private String description;
    private String offsetGenerator;
    private List<DataCollectionRun> runs;

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
