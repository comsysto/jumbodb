package org.jumbodb.benchmark.generator;


import com.google.common.collect.Lists;
import com.sun.tools.internal.ws.processor.util.DirectoryUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class DataCreator {

    private static final String FILE_NAME_FORMAT = "part%05d";
    private static final String JSON_DOC_PREFIX = "{\"name\": \"";
    private static final String JSON_DOC_SUFFIX = "\"";
    private static final int NR_OF_BUFFERED_JSON_DOCS = 10000;

    private final String outputFolder;
    private final int numberOfFiles;
    private final int dataSetsPerFile;
    private final int dataSetSizeInChars;
    private final String collectionName;
    private final int parrallelThreads;


    public DataCreator(String outputFolder, int numberOfFiles, int dataSetsPerFile, int dataSetSizeInChars,
                       String collectionName, int parallelThreads) {
        this.outputFolder = outputFolder;
        this.numberOfFiles = numberOfFiles;
        this.dataSetsPerFile = dataSetsPerFile;
        this.dataSetSizeInChars = dataSetSizeInChars;
        this.collectionName = collectionName;
        this.parrallelThreads = parallelThreads;
    }

    public void generateData() {

        final byte [][] randomizedJSONDocs = generateRandomizedJSONDocs(dataSetSizeInChars);
        List<Callable<Void>> generationRunners = Lists.newArrayList();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileName = generateDataFileName(outputFolder, collectionName, fileNo);
            generationRunners.add(new DataGeneratorRunner(fileName, dataSetsPerFile, randomizedJSONDocs));
        }
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(nrOfThreadsToUse());
            executorService.invokeAll(generationRunners);
        } catch (InterruptedException e) {
            // ULF that needs to be fixed
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    // ULF: use property to define it by your own
    private int nrOfThreadsToUse(){
        return parrallelThreads >= 1 ? parrallelThreads : Runtime.getRuntime().availableProcessors();
    }

    private String generateDataFileName(String outputFolder, String collectionName, int fileNr) {
        String path = FilenameUtils.concat(outputFolder, collectionName);
        // ULF evaluate return value
        new File(path).mkdir();
        return FilenameUtils.concat(path, String.format(FILE_NAME_FORMAT, fileNr));
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
