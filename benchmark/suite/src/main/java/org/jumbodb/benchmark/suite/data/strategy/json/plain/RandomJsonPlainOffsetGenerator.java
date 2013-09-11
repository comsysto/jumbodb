package org.jumbodb.benchmark.suite.data.strategy.json.plain;

import org.apache.commons.lang.math.RandomUtils;
import org.jumbodb.benchmark.suite.offsets.FileOffset;
import org.jumbodb.benchmark.suite.offsets.OffsetGenerator;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;
import org.jumbodb.common.util.file.DirectoryUtil;
import org.jumbodb.data.common.meta.ActiveProperties;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class RandomJsonPlainOffsetGenerator implements OffsetGenerator {

    private static final String ACTIVE_PROPERTIES_FILE = "active.properties";

    private String inputFolder;
    private String collectionName;
    private String chunkKey;
    private int samplesPerRun;


    @Override
    public void configure(BenchmarkJob benchmarkJob) {
        this.inputFolder = benchmarkJob.getInputFolder().getAbsolutePath();
        this.collectionName = benchmarkJob.getCollectionName();
        this.chunkKey = benchmarkJob.getChunkKey();
        this.samplesPerRun = benchmarkJob.getNumberOfSamplesPerRun();
    }

    @Override
    public List<FileOffset> getFileOffsets() throws IOException {
        String deliveryVersion = readDeliveryVersion(inputFolder, collectionName, chunkKey);
        File[] dataFiles = listDataFiles(getDataFilesPath(deliveryVersion));

        int defaultSampleCount = calculateSampleCount(dataFiles.length, samplesPerRun, false);
        int lastSampleCount = calculateSampleCount(dataFiles.length, samplesPerRun, true);

        return generateFileOffsets(dataFiles, defaultSampleCount, lastSampleCount);
    }

    private List<FileOffset> generateFileOffsets(File[] dataFiles, int defaultSampleCount, int lastSampleCount) {
        List<FileOffset> result = new ArrayList<FileOffset>(dataFiles.length * defaultSampleCount);

        for (int i=0; i<dataFiles.length; i++) {
            File dataFile = dataFiles[i];
            boolean lastFile = (i + 1) == dataFiles.length;

            for (int n=0; n < (lastFile ? lastSampleCount : defaultSampleCount); n++) {
                long offset = (long)(RandomUtils.nextDouble() * dataFile.length());
                result.add(new FileOffset(dataFile, offset));
            }
        }
        return result;
    }

    private String readDeliveryVersion(String inputFolder, String collectionName, String chunkKey) throws IOException {
        File activePropertiesFile = DirectoryUtil.concatenatePaths(inputFolder, collectionName, chunkKey,
                ACTIVE_PROPERTIES_FILE);
        return ActiveProperties.getActiveDeliveryVersion(activePropertiesFile);
    }

    private File[] listDataFiles(File dataFilesPath) {
        return DirectoryUtil.listDataFiles(dataFilesPath);
    }

    private File getDataFilesPath(String deliveryVersion) {
        return DirectoryUtil.concatenatePaths(inputFolder, collectionName, chunkKey, deliveryVersion);
    }

    private int calculateSampleCount(int dataFileCount, int samplesPerRun, boolean lastFile) {
        int defaultSamplesPerFile = samplesPerRun / dataFileCount;
        if (!lastFile) {
            return defaultSamplesPerFile;
        }
        int modulo = samplesPerRun % dataFileCount;
        return modulo == 0 ? defaultSamplesPerFile : modulo;
    }
}