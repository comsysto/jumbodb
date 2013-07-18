package org.jumbodb.benchmark.generator.config;

import java.util.List;

public class GeneratorConfig {

   private String description;
   private String outputFolder;
   private int parallelGenerationThreads;
   private List<Collection> collections;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public List<Collection> getCollections() {
        return collections;
    }

    public void setCollections(List<Collection> collections) {
        this.collections = collections;
    }

    public int getParallelGenerationThreads() {
        return parallelGenerationThreads;
    }

    public void setParallelGenerationThreads(int parallelGenerationThreads) {
        this.parallelGenerationThreads = parallelGenerationThreads;
    }
}
