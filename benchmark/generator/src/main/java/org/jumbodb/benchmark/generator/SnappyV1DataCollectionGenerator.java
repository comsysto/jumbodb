package org.jumbodb.benchmark.generator;

import org.apache.commons.io.FilenameUtils;
import org.jumbodb.benchmark.generator.runner.SnappyV1DataFileGenerationRunner;
import org.jumbodb.data.common.meta.CollectionProperties;

import java.io.File;
import java.util.Date;
import java.util.concurrent.Callable;

/**
 * @author Ulf Gitschthaler
 */
public class SnappyV1DataCollectionGenerator extends DataCollectionGenerator {

    private static final String DELIVERY_PATH = "created_by_generator";
    private static final String DELIVERY_STRATEGY = "JSON_SNAPPY_V1";


    public SnappyV1DataCollectionGenerator(String outputFolder, int numberOfFiles, int dataSetsPerFile,
            int dataSetSizeInChars, String collectionName, int parallelThreads) {
        super(outputFolder, numberOfFiles, dataSetsPerFile, dataSetSizeInChars, collectionName, parallelThreads);
    }

    @Override
    public Callable<Void> createDataGenerationRunner(String fileName, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
        return new SnappyV1DataFileGenerationRunner(fileName, dataSetsPerFile, randomizedJSONDocs);
    }

    @Override
    public void createDeliveryProperties(String dataFolder, String deliveryVersion, String description){
        CollectionProperties.CollectionMeta collectionMeta = new CollectionProperties.CollectionMeta("date",
                DELIVERY_PATH, DELIVERY_STRATEGY);

        String deliveryPropertiesPath = FilenameUtils.concat(dataFolder, CollectionProperties.DEFAULT_FILENAME);
        CollectionProperties.write(new File(deliveryPropertiesPath), collectionMeta);
    }
}
