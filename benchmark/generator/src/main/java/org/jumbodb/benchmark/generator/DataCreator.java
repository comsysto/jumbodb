package org.jumbodb.benchmark.generator;


import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class DataCreator {

    private static final String FILE_NAME_FORMAT = "part%05d";
    private static final String JSON_DOC_PREFIX = "{\"name\": \"";
    private static final String JSON_DOC_SUFFIX = "\"";
    private static final int NR_OF_BUFFERED_JSON_DOCS = 10000;

    //ULF refactor
    public void create(final String outputFolder, final int numberOfFiles, final int dataSetsPerFile,
                       final int dataSetSizeInChars) throws IOException {

        final byte [][] randomizedJSONDocs = createRandomizedJSONDocs(dataSetSizeInChars);
        int cores = Runtime.getRuntime().availableProcessors();

        ExecutorService executorService = Executors.newFixedThreadPool(cores);
        List<Callable<Void>> callables = Lists.newArrayList();

        for (long fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            final String fileName = String.format(FILE_NAME_FORMAT, fileNo);

            callables.add(
            new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(new File(outputFolder, fileName)));

                    for (int dataSetNo = 0, jsonDocNo = 0; dataSetNo < dataSetsPerFile; dataSetNo++){
                        fos.write(randomizedJSONDocs[jsonDocNo]);
                        jsonDocNo = jsonDocNo < NR_OF_BUFFERED_JSON_DOCS ? jsonDocNo + 1 : 0;
                    }
                    IOUtils.closeQuietly(fos);
                    return null;
                }
            });
        }
        try {
            executorService.invokeAll(callables);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    private byte[][] createRandomizedJSONDocs(int dataSetSizeInChars){
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
