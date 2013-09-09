package org.jumbodb.benchmark.generator.job;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.benchmark.generator.DataCollectionGenerator;
import org.jumbodb.benchmark.generator.PlainDataCollectionGenerator;
import org.jumbodb.benchmark.generator.SnappyV1DataCollectionGenerator;
import org.jumbodb.benchmark.generator.config.Collection;
import org.jumbodb.benchmark.generator.config.GeneratorConfig;

import java.io.File;
import java.io.IOException;

public class DataDeliveryGenerator {

    private static final String USER_HOME_PLACEHOLDER_1 = "$USER_HOME";
    private static final String USER_HOME_PLACEHOLDER_2 = "%USER_HOME%";

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
        GeneratorConfig generatorConfig = new ObjectMapper().readValue(configFile, GeneratorConfig.class);
        String outputFolder = adaptOutputFolder(generatorConfig.getOutputFolder());
        generatorConfig.setOutputFolder(outputFolder);
        return generatorConfig;
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

    private String adaptOutputFolder(String outputFolder) {
        if (!userHomePlaceholderPresent(outputFolder)) {
            return outputFolder;
        }
        return replaceUserHome(outputFolder);
    }

    private boolean userHomePlaceholderPresent(String outputFolder) {
        return StringUtils.contains(outputFolder, "$USER_HOME") || StringUtils.contains(outputFolder, "%USER_HOME%");
    }

    private String replaceUserHome(String outputFolder) {
        String destination = StringUtils.remove(outputFolder, USER_HOME_PLACEHOLDER_1);
        destination = StringUtils.remove(destination, USER_HOME_PLACEHOLDER_2);
        destination = destination.startsWith("/") ? destination.substring(1) : destination;

        return FilenameUtils.concat(System.getProperty("user.home"), destination);
    }
}
