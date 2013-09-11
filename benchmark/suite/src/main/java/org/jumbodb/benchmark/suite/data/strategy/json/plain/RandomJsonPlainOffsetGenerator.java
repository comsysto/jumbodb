package org.jumbodb.benchmark.suite.data.strategy.json.plain;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.jumbodb.benchmark.suite.offsets.FileOffset;
import org.jumbodb.benchmark.suite.offsets.OffsetGenerator;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Carsten Hufe
 */
public class RandomJsonPlainOffsetGenerator implements OffsetGenerator {

    private static final String ACTIVE_PROPERTIES_FILE = "active.properties";
    private static final String DELIVERY_VERSION_KEY = "deliveryVersion";

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
        File activePropertiesFile = concatenatePaths(inputFolder, collectionName, chunkKey,
                ACTIVE_PROPERTIES_FILE);

        Properties props = new Properties();
        props.load(new FileInputStream(activePropertiesFile));
        return (String) props.get(DELIVERY_VERSION_KEY);
    }

    private File[] listDataFiles(File dataFilesPath) {
        return dataFilesPath.listFiles(new DataFileFilter());
    }

    private File getDataFilesPath(String deliveryVersion) {
        return concatenatePaths(inputFolder, collectionName, chunkKey, deliveryVersion);
    }

    private int calculateSampleCount(int dataFileCount, int samplesPerRun, boolean lastFile) {
        int defaultSamplesPerFile = samplesPerRun / dataFileCount;
        if (!lastFile) {
            return defaultSamplesPerFile;
        }
        int modulo = samplesPerRun % dataFileCount;
        return modulo == 0 ? defaultSamplesPerFile : modulo;
    }

    private File concatenatePaths(String ... pathFragments) {
        if (ArrayUtils.isEmpty(pathFragments)) {
            return null;
        }
        File result = new File(pathFragments[0]);
        if (ArrayUtils.getLength(pathFragments) == 1){
            return result;
        }
        for (int i = 1; i < pathFragments.length; i++) {
            result = new File(result, pathFragments[i]);
        }
        return result;
    }
}


class DataFileFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
        return !(name.endsWith(".properties") || name.contains("_SUCCESS") || name.endsWith(".snappy"));
    }
}