package org.jumbodb.benchmark.suite.result;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.benchmark.suite.data.strategy.DataStrategies;
import org.jumbodb.benchmark.suite.data.strategy.DataStrategy;
import org.jumbodb.benchmark.suite.offsets.FileOffset;
import org.jumbodb.benchmark.suite.offsets.OffsetGenerator;

import java.io.File;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class BenchmarkJob {
    private File inputFolder;
    private Class<? extends OffsetGenerator> offsetGenerator;
    private String strategy;
    private String collection;
    private int numberOfThreads;
    private int datasetLoadGroupSize;
    private int numberOfSamplesPerRun;

    public BenchmarkJob(File inputFolder, Class<? extends OffsetGenerator> offsetGenerator,  String strategy, String collection, int numberOfThreads, int datasetLoadGroupSize, int numberOfSamplesPerRun) {
        this.inputFolder = inputFolder;
        this.offsetGenerator = offsetGenerator;
        this.strategy = strategy;
        this.collection = collection;
        this.numberOfThreads = numberOfThreads;
        this.datasetLoadGroupSize = datasetLoadGroupSize;
        this.numberOfSamplesPerRun = numberOfSamplesPerRun;
    }

    public BenchmarkJobResult run() {
        BenchmarkJobResult benchmarkResult;
        try {
            // prepare
            OffsetGenerator generator = offsetGenerator.newInstance();
            generator.configure(this);
            List<FileOffset> fileOffsets = generator.getFileOffsets();
            Class<? extends DataStrategy> dataStrategyClass = DataStrategies.getStrategy(strategy);
            DataStrategy dataStrategy = dataStrategyClass.newInstance();
            dataStrategy.configure(fileOffsets, this);
            // run
            benchmarkResult = dataStrategy.execute();
            // cleanup
            dataStrategy.cleanup();
        } catch (Exception e) {
            throw new UnhandledException(e);
        }
        return benchmarkResult;
    }

    public File getInputFolder() {
        return inputFolder;
    }

    public Class<? extends OffsetGenerator> getOffsetGenerator() {
        return offsetGenerator;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getCollection() {
        return collection;
    }

    public int getDatasetLoadGroupSize() {
        return datasetLoadGroupSize;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public int getNumberOfSamplesPerRun() {
        return numberOfSamplesPerRun;
    }

    @Override
    public String toString() {
        return "BenchmarkJob{" +
                "offsetGenerator=" + offsetGenerator +
                ", strategy='" + strategy + '\'' +
                ", collection='" + collection + '\'' +
                ", numberOfThreads=" + numberOfThreads +
                ", datasetLoadGroupSize=" + datasetLoadGroupSize +
                ", numberOfSamplesPerRun=" + numberOfSamplesPerRun +
                '}';
    }
}
