package org.jumbodb.benchmark.suite.config;

import java.util.List;

public class BenchmarkSuiteConfig {

    private String description;
    private String inputFolder;
    private String resultFile;

    private List<DataCollection> dataCollections;

    public String getResultFile() {
        return resultFile;
    }

    public void setResultFile(String resultFile) {
        this.resultFile = resultFile;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getInputFolder() {
        return inputFolder;
    }

    public void setInputFolder(String inputFolder) {
        this.inputFolder = inputFolder;
    }

    public List<DataCollection> getDataCollections() {
        return dataCollections;
    }

    public void setDataCollections(List<DataCollection> dataCollections) {
        this.dataCollections = dataCollections;
    }
}
