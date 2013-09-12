package org.jumbodb.benchmark.suite.result;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.benchmark.suite.data.strategy.DataStrategies;
import org.jumbodb.benchmark.suite.data.strategy.DataStrategy;
import org.jumbodb.benchmark.suite.offset.FileOffset;
import org.jumbodb.benchmark.suite.offset.generator.OffsetGenerator;

import java.io.File;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class BenchmarkJob {
    private File inputFolder;
    private Class<? extends OffsetGenerator> offsetGenerator;
    private String strategy;
    private String collectionName;
    private String chunkKey;
    private int numberOfThreads;
    private int datasetLoadGroupSize;
    private int numberOfSamplesPerRun;
    private int averageDataSetSize;

    public BenchmarkJob(File inputFolder, Class<? extends OffsetGenerator> offsetGenerator, String strategy, String collection, String chunkKey, int numberOfThreads, int datasetLoadGroupSize, int numberOfSamplesPerRun, int averageDataSetSize) {
        this.inputFolder = inputFolder;
        this.offsetGenerator = offsetGenerator;
        this.strategy = strategy;
        this.collectionName = collection;
        this.chunkKey = chunkKey;
        this.numberOfThreads = numberOfThreads;
        this.datasetLoadGroupSize = datasetLoadGroupSize;
        this.numberOfSamplesPerRun = numberOfSamplesPerRun;
        this.averageDataSetSize = averageDataSetSize;
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

    public String getCollectionName() {
        return collectionName;
    }

    public String getChunkKey() {
        return chunkKey;
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

    public int getAverageDataSetSize() {
        return averageDataSetSize;
    }

    @Override
    public String toString() {
        return "BenchmarkJob{" +
                "offsetGenerator=" + offsetGenerator +
                ", strategy='" + strategy + '\'' +
                ", collection='" + collectionName + '\'' +
                ", numberOfThreads=" + numberOfThreads +
                ", datasetLoadGroupSize=" + datasetLoadGroupSize +
                ", numberOfSamplesPerRun=" + numberOfSamplesPerRun +
                '}';
    }
}
