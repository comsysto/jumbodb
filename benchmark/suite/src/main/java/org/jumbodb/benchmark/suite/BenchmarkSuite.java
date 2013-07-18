package org.jumbodb.benchmark.suite;


import org.jumbodb.benchmark.suite.config.BenchmarkSuiteConfig;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;
import org.jumbodb.benchmark.suite.result.BenchmarkJobResult;

import java.util.List;


public class BenchmarkSuite {


    public List<BenchmarkJob> createBenchmarkJobs(BenchmarkSuiteConfig config) {
        return null;
    }

    public void run(BenchmarkSuiteConfig config) {
        List<BenchmarkJob> benchmarkJobs = createBenchmarkJobs(config);
        for (BenchmarkJob benchmarkJob : benchmarkJobs) {
            System.out.println("Starting " + benchmarkJob);
            BenchmarkJobResult run = benchmarkJob.run();
            // CARSTEN write result to file

            System.out.println("Finished " + benchmarkJob);

        }
    }
}
