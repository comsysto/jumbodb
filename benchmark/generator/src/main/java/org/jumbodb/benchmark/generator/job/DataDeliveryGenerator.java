package org.jumbodb.benchmark.generator.job;

import org.apache.commons.lang.ArrayUtils;
import org.jumbodb.benchmark.generator.DataCollectionGenerator;
import org.jumbodb.benchmark.generator.PlainDataCollectionGenerator;
import org.jumbodb.benchmark.generator.SnappyV1DataCollectionGenerator;
import org.jumbodb.benchmark.generator.config.Collection;
import org.jumbodb.benchmark.generator.config.GenerationContext;
import org.jumbodb.benchmark.generator.config.GeneratorConfig;
import org.jumbodb.common.util.config.JSONConfigReader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class DataDeliveryGenerator {

    private static DataDeliveryGenerator dataGenerator = new DataDeliveryGenerator();


    public static void main(String[] args) throws IOException {
        if (!checkConfigParams(args)) {
            throw new IllegalArgumentException("Data generator job expects config file start parameter");
        }
        dataGenerator.run(args[0]);
    }

    protected void run(String configFile) throws IOException {
        GeneratorConfig config = parseConfigFile(configFile);
        CountDownLatch creationCounter = initCreationCounter(config);

        for (Collection collection : config.getCollections()) {
            DataCollectionGenerator dataCollectionGenerator;

            if("JSON_PLAIN_V1".equals(collection.getStrategy())){
                dataCollectionGenerator = new PlainDataCollectionGenerator(buildContext(config, collection,
                        creationCounter));

            } else if("JSON_SNAPPY_V1".equals(collection.getStrategy())){
                dataCollectionGenerator =  new SnappyV1DataCollectionGenerator(buildContext(config, collection,
                        creationCounter));
            } else {
                throw new IllegalStateException("Strategy " + collection.getStrategy() + " does not exist");
            }
            dataCollectionGenerator.generateData();
        }
    }

    protected GeneratorConfig parseConfigFile(String configFile) throws IOException {
        return JSONConfigReader.read(GeneratorConfig.class, configFile);
    }

    private CountDownLatch initCreationCounter(GeneratorConfig config) {
        int startValue = 0;

        for (Collection collection : config.getCollections()) {
            startValue += collection.getNumberOfFiles();

        }
        return new CountDownLatch(startValue);
    }

    private GenerationContext buildContext(GeneratorConfig config, Collection collection, CountDownLatch creationCounter) {
        return new GenerationContext(config, collection, creationCounter);
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
