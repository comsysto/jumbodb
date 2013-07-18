package org.jumbodb.benchmark.generator;

import org.apache.commons.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.Callable;


public class DataFileGenerationRunner implements Callable<Void>{

    private final String destFilePath;
    private final int dataSetsPerFile;
    private final byte[][] jsonDocs;


   public DataFileGenerationRunner(String outputFilePath, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
        this.destFilePath = outputFilePath;
        this.dataSetsPerFile = dataSetsPerFile;
        this.jsonDocs = randomizedJSONDocs;
    }

    @Override
    public Void call() throws Exception {
        FileOutputStream fos = new FileOutputStream(new File(destFilePath));
        BufferedOutputStream bos = new BufferedOutputStream(fos);

        for (int dataSetNo = 0, jsonDocNo = 0; dataSetNo < dataSetsPerFile; dataSetNo++){
            fos.write(jsonDocs[jsonDocNo]);
            jsonDocNo = jsonDocNo < jsonDocs.length ? jsonDocNo + 1 : 0;
        }
        IOUtils.closeQuietly(bos);
        IOUtils.closeQuietly(fos);
        return null;
    }
}
