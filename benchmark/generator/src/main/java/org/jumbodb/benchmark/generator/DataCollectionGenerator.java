package org.jumbodb.benchmark.generator;


import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.jumbodb.benchmark.generator.config.GenerationContext;
import org.jumbodb.common.util.file.DirectoryUtil;
import org.jumbodb.data.common.meta.ActiveProperties;

import java.io.File;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public abstract class DataCollectionGenerator {

    private static final String FILE_NAME_FORMAT = "part%05d";
    private static final String JSON_DOC_PREFIX = "{\"name\": \"";
    private static final String JSON_DOC_SUFFIX = "\"}";
    private static final int NR_OF_BUFFERED_JSON_DOCS = 10000;
    private static final String DESCRIPTION = "Benchmark delivery";
    private static final String ACTIVE_PROPERTIES_FILE_NAME = "active.properties";
    public static final String DEFAULT_CHUNK_NAME = "benchmark_delivery";

    private final GenerationContext context;


    protected DataCollectionGenerator(GenerationContext context) {
        this.context = context;
    }

    public void generateData() {
        String chunkKeyDir = getChunkKeyDir(context.getOutputFolder(), context.getCollectionName());
        String dataFileDir = getDataDir(context.getOutputFolder(), context.getCollectionName());

        createDataFolder(dataFileDir);
        createDeliveryProperties(dataFileDir, context.getDeliveryVersion(), DESCRIPTION);
        createActiveProperties(chunkKeyDir, context.getDeliveryVersion());

        final byte [][] randomizedJSONDocs = generateRandomizedJSONDocs(context.getDataSetSizeInChars());
        List<Callable<Void>> generationRunners = Lists.newArrayList();

        for (int fileNo = 0; fileNo < context.getNumberOfFiles(); fileNo++) {
            String fileName = getDataFileName(dataFileDir, fileNo);
            generationRunners.add(createDataGenerationRunner(fileName, context, randomizedJSONDocs));
        }
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(nrOfThreadsToUse());
            executorService.invokeAll(generationRunners);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract Callable<Void> createDataGenerationRunner(String fileName, GenerationContext dataSetsPerFile, byte[][] randomizedJSONDocs);

    public abstract void createDeliveryProperties(String dataFolder, String deliveryVersion, String description);


    private int nrOfThreadsToUse(){
        return context.getParallelThreads() >= 1 ? context.getParallelThreads() : Runtime.getRuntime().availableProcessors();
    }

    private void createDataFolder(String dataFolder) {
        if (!new File(dataFolder).mkdirs()){
            throw new IllegalStateException("Cannot create data folders");
        }
    }

    private String getChunkKeyDir(String outputFolder, String collectionName) {
        return DirectoryUtil.concatenatePaths(outputFolder, collectionName, DEFAULT_CHUNK_NAME).getAbsolutePath();
    }

    private String getDataDir(String outputFolder, String collectionName) {
        return DirectoryUtil.concatenatePaths(outputFolder, collectionName, DEFAULT_CHUNK_NAME,
                context.getDeliveryVersion()).getAbsolutePath();
    }

    private String getDataFileName(String dataFileFolder, int fileNr) {
        return FilenameUtils.concat(dataFileFolder, String.format(FILE_NAME_FORMAT, fileNr));
    }

    private byte[][] generateRandomizedJSONDocs(int dataSetSizeInChars){
        int randomStringSize = calculateRandomStringLength(dataSetSizeInChars);
        String randomString = generateRandomString(randomStringSize);
        byte [][] result = new byte[NR_OF_BUFFERED_JSON_DOCS][];

        for (int i=0; i< NR_OF_BUFFERED_JSON_DOCS; i++) {
            int beginIndex = i * randomStringSize;
            int endIndex = beginIndex + randomStringSize;

            result[i] = generateJSONDocument(randomString, beginIndex, endIndex).getBytes();
        }
        return result;
    }

    private String generateJSONDocument(String randomString, int beginIndex, int endIndex) {
        return JSON_DOC_PREFIX + randomString.substring(beginIndex, endIndex) + JSON_DOC_SUFFIX
                + System.getProperty("line.separator");
    }

    private String generateRandomString(int randomStringSize) {
        return RandomStringUtils.random(randomStringSize * NR_OF_BUFFERED_JSON_DOCS);
    }

    private int calculateRandomStringLength(int dataSetSizeInChars) {
        return dataSetSizeInChars - (JSON_DOC_SUFFIX.length() + JSON_DOC_PREFIX.length());
    }

    private void createActiveProperties(String chunkKeyDir, String deliveryVersion) {
        File activePropertiesFile = DirectoryUtil.concatenatePaths(chunkKeyDir, ACTIVE_PROPERTIES_FILE_NAME);
        ActiveProperties.writeActiveFile(activePropertiesFile, deliveryVersion);
    }
}
