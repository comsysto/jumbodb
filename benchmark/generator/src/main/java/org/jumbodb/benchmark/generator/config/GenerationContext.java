package org.jumbodb.benchmark.generator.config;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class GenerationContext {

    private final GeneratorConfig config;
    private final Collection collection;
    private final CountDownLatch creationCounter;

    private final String deliveryVersion;
    private boolean finished;


    public GenerationContext(GeneratorConfig config, Collection collection, CountDownLatch creationCounter) {
        this.config = config;
        this.collection = collection;
        this.creationCounter = creationCounter;

        this.finished = false;
        this.deliveryVersion = UUID.randomUUID().toString();
    }

    public final void finishGeneration(){
        if (finished) {
            throw new UnsupportedOperationException("Generation already finished!");
        }
        creationCounter.countDown();
        finished = true;
    }

    public final String getOutputFolder() {
        return config.getOutputFolder();
    }

    public final String getCollectionName() {
        return collection.getName();
    }

    public final int getDataSetSizeInChars(){
        return collection.getDataSetSizeInChars();
    }

    public final int getNumberOfFiles(){
        return collection.getNumberOfFiles();
    }

    public final int getDataSetsPerFile() {
        return collection.getDataSetsPerFile();
    }

    public final int getParallelThreads() {
        return config.getParallelGenerationThreads();
    }

    public final String getDeliveryVersion() {
        return deliveryVersion;
    }
}
