package org.jumbodb.benchmark.generator;

import org.apache.commons.lang.ArrayUtils;

import java.io.File;

/**
 * @author Ulf Gitschthaler
 */
public class DataGenerator {

    private static DataGenerator dataGenerator = new DataGenerator();


    public static void main(String[] args) {
        if (!checkConfigParams(args)) {
            throw new IllegalArgumentException("Data generator job expects config file start parameter");
        }
        dataGenerator.run(new File(args[0]));
    }

    private static boolean checkConfigParams(String[] args) {
        return isRequiredParameterPresent(args) && isConfigFilePresent(args[0]);
    }

    protected void run(File configFile) {
        throw new IllegalStateException("Not implemented");
    }

    private static boolean isConfigFilePresent(String arg) {
        File configFile = new File(arg);
        return configFile.exists() && configFile.isFile();
    }

    private static boolean isRequiredParameterPresent(String[] args) {
        return ArrayUtils.getLength(args) == 1;
    }


}
