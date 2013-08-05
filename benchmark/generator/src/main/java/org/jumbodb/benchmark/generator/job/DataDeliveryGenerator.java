package org.jumbodb.benchmark.generator.job;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.benchmark.generator.DataCollectionGenerator;
import org.jumbodb.benchmark.generator.SnappyV1DataCollectionGenerator;
import org.jumbodb.benchmark.generator.config.Collection;
import org.jumbodb.benchmark.generator.config.GeneratorConfig;
import org.jumbodb.benchmark.generator.PlainDataCollectionGenerator;

import java.io.File;
import java.io.IOException;

public class DataDeliveryGenerator {

    private static DataDeliveryGenerator dataGenerator = new DataDeliveryGenerator();

    public static void main(String[] args) throws IOException {
        if (!checkConfigParams(args)) {
            throw new IllegalArgumentException("Data generator job expects config file start parameter");
        }
        dataGenerator.run(new File(args[0]));
    }

    protected void run(File configFile) throws IOException {
        GeneratorConfig config = parseConfigFile(configFile);

        for (Collection collection : config.getCollections()) {
            DataCollectionGenerator dataCollectionGenerator;

            if("JSON_PLAIN_V1".equals(collection.getStrategy())){
                dataCollectionGenerator = new PlainDataCollectionGenerator(config.getOutputFolder(),
                        collection.getNumberOfFiles(), collection.getDataSetsPerFile(), collection.getDataSetSizeInChars(),
                        collection.getName(), config.getParallelGenerationThreads());
            } else if("JSON_SNAPPY_V1".equals(collection.getStrategy())){
                dataCollectionGenerator =  new SnappyV1DataCollectionGenerator(config.getOutputFolder(),
                        collection.getNumberOfFiles(), collection.getDataSetsPerFile(), collection.getDataSetSizeInChars(),
                        collection.getName(), config.getParallelGenerationThreads());
            } else {
                throw new IllegalStateException("Strategy " + collection.getStrategy() + " does not exist");
            }
            dataCollectionGenerator.generateData();
        }
    }

    protected GeneratorConfig parseConfigFile(File configFile) throws IOException {
        return new ObjectMapper().readValue(configFile, GeneratorConfig.class);
    }

    private static boolean checkConfigParams(String[] args) {
        return isRequiredParameterPresent(args) && isConfigFilePresent(args[0]);
    }

    private static boolean isConfigFilePresent(String arg) {
        File configFile = new File(arg);
        return configFile.exists() && configFile.isFile();
    }

    private static boolean isRequiredParameterPresent(String[] args) {
        return ArrayUtils.getLength(args) == 1;
    }
}
