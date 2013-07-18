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
    private static final String JSON_DOC_FORMAT = "{\"name\": \"%s\"}%n";


    //ULF refactor
    public void create(final String outputFolder, final int numberOfFiles, final int dataSetsPerFile,
                       final int dataSetSizeInByte) throws IOException {


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

                    for (long dataSetNo = 0; dataSetNo < dataSetsPerFile; dataSetNo++){
                        String jsonDocument = createJSONDocument(dataSetSizeInByte);
                        fos.write(jsonDocument.getBytes());
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

    protected String createJSONDocument(int docSizeInBytes) {
        int remaining = docSizeInBytes - JSON_DOC_FORMAT.length() + 3;
        return String.format(JSON_DOC_FORMAT, RandomStringUtils.random(remaining));
    }
}
