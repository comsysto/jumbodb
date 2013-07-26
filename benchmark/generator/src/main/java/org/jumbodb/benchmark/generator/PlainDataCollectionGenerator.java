package org.jumbodb.benchmark.generator;


import org.apache.commons.io.FilenameUtils;
import org.jumbodb.benchmark.generator.DataCollectionGenerator;
import org.jumbodb.benchmark.generator.runner.DataFileGenerationRunner;
import org.jumbodb.benchmark.generator.runner.PlainDataFileGenerationRunner;
import org.jumbodb.data.common.meta.DeliveryProperties;

import java.io.File;
import java.util.Date;

public class PlainDataCollectionGenerator extends DataCollectionGenerator {

    public PlainDataCollectionGenerator(String outputFolder, int numberOfFiles, int dataSetsPerFile, int dataSetSizeInChars,
            String collectionName, int parallelThreads) {
        super(outputFolder, numberOfFiles, dataSetsPerFile, dataSetSizeInChars, collectionName, parallelThreads);
    }

    @Override
    public DataFileGenerationRunner createDataGenerationRunner(String fileName, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
        return new PlainDataFileGenerationRunner(fileName, dataSetsPerFile, randomizedJSONDocs);
    }

    @Override
    public void createDeliveryProperties(String dataFolder, String deliveryVersion, String description){
        DeliveryProperties.DeliveryMeta deliveryMeta = new DeliveryProperties.DeliveryMeta(deliveryVersion, description, new Date().toString(),
                "/dev/null", DEFAULT_CHUNK_NAME);

        String deliveryPropertiesPath = FilenameUtils.concat(dataFolder, DeliveryProperties.DEFAULT_FILENAME);
        DeliveryProperties.write(new File(deliveryPropertiesPath), deliveryMeta);
    }
}