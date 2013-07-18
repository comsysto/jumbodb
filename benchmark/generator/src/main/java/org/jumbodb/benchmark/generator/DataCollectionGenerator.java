package org.jumbodb.benchmark.generator;


import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.jumbodb.data.common.meta.DeliveryProperties;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.jumbodb.data.common.meta.DeliveryProperties.*;


public class DataCollectionGenerator {

    private static final String FILE_NAME_FORMAT = "part%05d";
    private static final String JSON_DOC_PREFIX = "{\"name\": \"";
    private static final String JSON_DOC_SUFFIX = "\"";
    private static final int NR_OF_BUFFERED_JSON_DOCS = 10000;
    private static final String DESCRIPTION = "Benchmark delivery";
    public static final String DEFAULT_CHUNK_NAME = "benchmark_delivery";

    private final String outputFolder;
    private final int numberOfFiles;
    private final int dataSetsPerFile;
    private final int dataSetSizeInChars;
    private final String collectionName;
    private final String deliveryVersion = UUID.randomUUID().toString();
    private final int parallelThreads;


    public DataCollectionGenerator(String outputFolder, int numberOfFiles, int dataSetsPerFile, int dataSetSizeInChars,
                                   String collectionName, int parallelThreads) {
        this.outputFolder = outputFolder;
        this.numberOfFiles = numberOfFiles;
        this.dataSetsPerFile = dataSetsPerFile;
        this.dataSetSizeInChars = dataSetSizeInChars;
        this.collectionName = collectionName;
        this.parallelThreads = parallelThreads;
    }

    public void generateData() {
        String dataFolder = getDataFolder(outputFolder, collectionName);
        createDataFolder(dataFolder);
        createDeliveryProperties(dataFolder);

        final byte [][] randomizedJSONDocs = generateRandomizedJSONDocs(dataSetSizeInChars);
        List<Callable<Void>> generationRunners = Lists.newArrayList();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileName = getDataFileName(dataFolder, fileNo);
            generationRunners.add(new DataFileGenerationRunner(fileName, dataSetsPerFile, randomizedJSONDocs));
        }
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(nrOfThreadsToUse());
            executorService.invokeAll(generationRunners);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDeliveryProperties(String dataFolder){
        DeliveryMeta deliveryMeta = new DeliveryMeta(deliveryVersion, DESCRIPTION, new Date().toString(),
                "/dev/null", DEFAULT_CHUNK_NAME);

        String deliveryPropertiesPath = FilenameUtils.concat(dataFolder, DeliveryProperties.DEFAULT_FILENAME);
        DeliveryProperties.write(new File(deliveryPropertiesPath), deliveryMeta);
    }

    private int nrOfThreadsToUse(){
        return parallelThreads >= 1 ? parallelThreads : Runtime.getRuntime().availableProcessors();
    }

    private void createDataFolder(String dataFolder) {
        if (!new File(dataFolder).mkdirs()){
            throw new IllegalStateException("Cannot create data folders");
        }
    }

    private String getDataFolder(String outputFolder, String collectionName) {
        String collectionPath = FilenameUtils.concat(outputFolder, collectionName);
        String chunkPath = FilenameUtils.concat(collectionPath, DEFAULT_CHUNK_NAME);
        return FilenameUtils.concat(chunkPath, deliveryVersion);
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
}
