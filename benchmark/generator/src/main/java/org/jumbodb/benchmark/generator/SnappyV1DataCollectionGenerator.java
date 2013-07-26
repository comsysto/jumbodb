package org.jumbodb.benchmark.generator;

import org.jumbodb.benchmark.generator.runner.SnappyV1DataFileGenerationRunner;
import org.xerial.snappy.SnappyOutputStream;

import java.util.concurrent.Callable;

/**
 * @author Ulf Gitschthaler
 */
public class SnappyV1DataCollectionGenerator extends DataCollectionGenerator {

    public SnappyV1DataCollectionGenerator(String outputFolder, int numberOfFiles, int dataSetsPerFile, int dataSetSizeInChars, String collectionName, int parallelThreads) {
        super(outputFolder, numberOfFiles, dataSetsPerFile, dataSetSizeInChars, collectionName, parallelThreads);
    }

    @Override
    public Callable<Void> createDataGenerationRunner(String fileName, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
        return new SnappyV1DataFileGenerationRunner(fileName, dataSetsPerFile, randomizedJSONDocs);
    }

    @Override
    public void createDeliveryProperties(String dataFolder, String deliveryVersion, String description) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
