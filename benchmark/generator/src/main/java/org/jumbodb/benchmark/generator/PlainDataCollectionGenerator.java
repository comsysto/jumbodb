package org.jumbodb.benchmark.generator;


import org.apache.commons.io.FilenameUtils;
import org.jumbodb.benchmark.generator.config.GenerationContext;
import org.jumbodb.benchmark.generator.runner.DataFileGenerationRunner;
import org.jumbodb.benchmark.generator.runner.PlainDataFileGenerationRunner;
import org.jumbodb.data.common.meta.CollectionProperties;

import java.io.File;
import java.util.Date;

public class PlainDataCollectionGenerator extends DataCollectionGenerator {

    private static final String DELIVERY_PATH = "created_by_generator";
    private static final String DELIVERY_STRATEGY = "JSON_PLAIN_V1";

    private final GenerationContext context;

    public PlainDataCollectionGenerator(GenerationContext context) {
        super(context);
        this.context = context;
    }

    @Override
    public DataFileGenerationRunner createDataGenerationRunner(String fileName, GenerationContext dataSetsPerFile, byte[][] randomizedJSONDocs) {
        return new PlainDataFileGenerationRunner(fileName, context, randomizedJSONDocs);
    }

    @Override
    public void createDeliveryProperties(String dataFolder, String deliveryVersion, String description){
        CollectionProperties.CollectionMeta collectionMeta = new CollectionProperties.CollectionMeta("date",
                DELIVERY_PATH, DELIVERY_STRATEGY, "info");

        String deliveryPropertiesPath = FilenameUtils.concat(dataFolder, CollectionProperties.DEFAULT_FILENAME);
        CollectionProperties.write(new File(deliveryPropertiesPath), collectionMeta);
    }
}