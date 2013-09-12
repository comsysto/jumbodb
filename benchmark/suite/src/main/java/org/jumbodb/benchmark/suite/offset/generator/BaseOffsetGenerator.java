package org.jumbodb.benchmark.suite.offset.generator;

import org.jumbodb.benchmark.suite.offset.FileOffset;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;
import org.jumbodb.common.util.file.DirectoryUtil;
import org.jumbodb.data.common.meta.ActiveProperties;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Ulf Gitschthaler
 */
public abstract class BaseOffsetGenerator implements OffsetGenerator {

    private static final String ACTIVE_PROPERTIES_FILE = "active.properties";

    private String inputFolder;
    private String collectionName;
    private String chunkKey;
    private int samplesPerRun;
    protected int averageDataSetSize;


    @Override
    public void configure(BenchmarkJob benchmarkJob) {
        this.inputFolder = benchmarkJob.getInputFolder().getAbsolutePath();
        this.collectionName = benchmarkJob.getCollectionName();
        this.chunkKey = benchmarkJob.getChunkKey();
        this.samplesPerRun = benchmarkJob.getNumberOfSamplesPerRun();
        this.averageDataSetSize = benchmarkJob.getAverageDataSetSize();
    }

    @Override
    public List<FileOffset> getFileOffsets() throws IOException {
        String deliveryVersion = readDeliveryVersion(inputFolder, collectionName, chunkKey);
        File[] dataFiles = listDataFiles(getDataFilesPath(deliveryVersion));

        int defaultSampleCount = calculateSampleCount(dataFiles.length, samplesPerRun, false);
        int lastSampleCount = calculateSampleCount(dataFiles.length, samplesPerRun, true);

        return generateFileOffsets(dataFiles, defaultSampleCount, lastSampleCount);
    }

    String readDeliveryVersion(String inputFolder, String collectionName, String chunkKey) throws IOException {
        File activePropertiesFile = DirectoryUtil.concatenatePaths(inputFolder, collectionName, chunkKey,
                ACTIVE_PROPERTIES_FILE);
        return ActiveProperties.getActiveDeliveryVersion(activePropertiesFile);
    }

    File[] listDataFiles(File dataFilesPath) {
        return DirectoryUtil.listDataFiles(dataFilesPath);
    }

    File getDataFilesPath(String deliveryVersion) {
        return DirectoryUtil.concatenatePaths(inputFolder, collectionName, chunkKey, deliveryVersion);
    }

    int calculateSampleCount(int dataFileCount, int samplesPerRun, boolean lastFile) {
        int defaultSamplesPerFile = samplesPerRun / dataFileCount;
        if (!lastFile) {
            return defaultSamplesPerFile;
        }
        int modulo = samplesPerRun % dataFileCount;
        return modulo == 0 ? defaultSamplesPerFile : modulo;
    }


    abstract List<FileOffset> generateFileOffsets(File[] dataFiles, int defaultSampleCount, int lastSampleCount);


    public String getInputFolder() {
        return inputFolder;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public int getAverageDataSetSize() {
        return averageDataSetSize;
    }

    public int getSamplesPerRun() {
        return samplesPerRun;
    }
}
