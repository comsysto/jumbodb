package org.jumbodb.benchmark.generator;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.benchmark.generator.config.Collection;
import org.jumbodb.benchmark.generator.config.GeneratorConfig;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * @author Ulf Gitschthaler
 */
public class DataGenerator {

    private static DataGenerator dataGenerator = new DataGenerator();

    public static void main(String[] args) throws IOException {
        if (!checkConfigParams(args)) {
            throw new IllegalArgumentException("Data generator job expects config file start parameter");
        }
        dataGenerator.run(new File(args[0]));
    }

    protected void run(File configFile) throws IOException {
        GeneratorConfig config = parseConfigFile(configFile);
        Collection collection = config.getCollections().get(0);

        new DataCreator().create(config.getOutputFolder(), collection.getNumberOfFiles(), collection.getDataSetsPerFile(),
                collection.getDataSetSizeInByte());
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
