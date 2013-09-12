package org.jumbodb.benchmark.suite;


import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.benchmark.suite.config.BenchmarkSuiteConfig;
import org.jumbodb.benchmark.suite.config.DataCollection;
import org.jumbodb.benchmark.suite.config.DataCollectionRun;
import org.jumbodb.benchmark.suite.offset.generator.OffsetGenerator;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;
import org.jumbodb.benchmark.suite.result.BenchmarkJobResult;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;


public class BenchmarkSuite {
    public List<BenchmarkJob> createBenchmarkJobs(BenchmarkSuiteConfig config) throws ClassNotFoundException {
        List<BenchmarkJob> benchmarkJobs = new LinkedList<BenchmarkJob>();
        List<DataCollection> dataCollections = config.getDataCollections();
        File inputFolder = new File(config.getInputFolder());
        for (DataCollection dataCollection : dataCollections) {
            for (DataCollectionRun dataCollectionRun : dataCollection.getRuns()) {
                for (Integer numberOfThreads : dataCollectionRun.getNumberOfThreads()) {
                    for (Integer datasetLoadGroupSize : dataCollectionRun.getDatasetLoadGroupSize()) {
                        String offsetGenerator = dataCollection.getOffsetGenerator();
                        Class<? extends OffsetGenerator> offsetGeneratorClazz = (Class<? extends OffsetGenerator>) Class.forName(offsetGenerator);
                        benchmarkJobs.add(new BenchmarkJob(inputFolder, offsetGeneratorClazz, dataCollectionRun.getStrategy(),
                                dataCollection.getCollectionName(), dataCollection.getChunkKey(), numberOfThreads, datasetLoadGroupSize, dataCollectionRun.getNumberOfSamplesPerRun(),
                                dataCollectionRun.getAverageDataSetSize()));
                    }
                }
            }
        }
        return benchmarkJobs;
    }

    public void run(BenchmarkSuiteConfig config) {
        FileOutputStream fos = null;
        PrintStream ps = null;
        try {
            fos = new FileOutputStream(config.getResultFile());
            ps = new PrintStream(fos);
            List<BenchmarkJob> benchmarkJobs = createBenchmarkJobs(config);
            for (BenchmarkJob benchmarkJob : benchmarkJobs) {
                System.out.println("Starting " + benchmarkJob);
                BenchmarkJobResult run = benchmarkJob.run();
                ps.println(run.toString());
                System.out.println("Finished " + benchmarkJob);
            }
        } catch(Exception e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(ps);
            IOUtils.closeQuietly(fos);
        }
    }
}
