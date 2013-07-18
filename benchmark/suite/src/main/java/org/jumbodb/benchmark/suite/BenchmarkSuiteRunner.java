package org.jumbodb.benchmark.suite;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.benchmark.suite.config.BenchmarkSuiteConfig;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class BenchmarkSuiteRunner {

    private static BenchmarkSuiteRunner benchmarkSuiteRunner = new BenchmarkSuiteRunner();

    public static void main(String[] args) throws IOException {
        if (!checkConfigParams(args)) {
            throw new IllegalArgumentException("BenchmarkSuiteRunner job expects config file start parameter");
        }
        benchmarkSuiteRunner.run(new File(args[0]));
    }

    protected void run(File configFile) throws IOException {
        BenchmarkSuiteConfig config = parseConfigFile(configFile);
        // create execution plans
        BenchmarkSuite suite = new BenchmarkSuite();
        suite.run(config);
    }

    protected BenchmarkSuiteConfig parseConfigFile(File configFile) throws IOException {
        return new ObjectMapper().readValue(configFile, BenchmarkSuiteConfig.class);
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
