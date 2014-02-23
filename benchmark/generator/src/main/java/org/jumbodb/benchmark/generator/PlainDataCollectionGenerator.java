package org.jumbodb.benchmark.generator;


import org.apache.commons.io.FilenameUtils;
import org.jumbodb.benchmark.generator.runner.DataFileGenerationRunner;
import org.jumbodb.benchmark.generator.runner.PlainDataFileGenerationRunner;
import org.jumbodb.data.common.meta.CollectionProperties;

import java.io.File;
import java.util.Date;

public class PlainDataCollectionGenerator extends DataCollectionGenerator {

    private static final String DELIVERY_PATH = "created_by_generator";
    private static final String DELIVERY_STRATEGY = "JSON_PLAIN_V1";

    public PlainDataCollectionGenerator(String outputFolder, int numberOfFiles, int dataSetsPerFile,
            int dataSetSizeInChars, String collectionName, int parallelThreads) {
        super(outputFolder, numberOfFiles, dataSetsPerFile, dataSetSizeInChars, collectionName, parallelThreads);
    }

    @Override
    public DataFileGenerationRunner createDataGenerationRunner(String fileName, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
        return new PlainDataFileGenerationRunner(fileName, dataSetsPerFile, randomizedJSONDocs);
    }

    @Override
    public void createDeliveryProperties(String dataFolder, String deliveryVersion, String description){
        CollectionProperties.CollectionMeta collectionMeta = new CollectionProperties.CollectionMeta("date",
                DELIVERY_PATH, DELIVERY_STRATEGY);

        String deliveryPropertiesPath = FilenameUtils.concat(dataFolder, CollectionProperties.DEFAULT_FILENAME);
        CollectionProperties.write(new File(deliveryPropertiesPath), collectionMeta);
    }
}