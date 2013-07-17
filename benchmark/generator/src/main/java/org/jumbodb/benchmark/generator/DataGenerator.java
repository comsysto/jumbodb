package org.jumbodb.benchmark.generator;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.benchmark.generator.config.ConfigFile;

import java.io.File;
import java.io.IOException;

/**
 * @author Ulf Gitschthaler
 */
public class DataGenerator {

    private static DataGenerator dataGenerator = new DataGenerator();
    private ConfigFile config;

    public static void main(String[] args) throws IOException {
        if (!checkConfigParams(args)) {
            throw new IllegalArgumentException("Data generator job expects config file start parameter");
        }
        dataGenerator.run(new File(args[0]));
    }

    protected void run(File configFile) throws IOException {
        this.config = parseConfigFile(configFile);

        throw new IllegalStateException("Not implemented");
    }

    protected ConfigFile parseConfigFile(File configFile) throws IOException {
        return new ObjectMapper().readValue(configFile, ConfigFile.class);
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
