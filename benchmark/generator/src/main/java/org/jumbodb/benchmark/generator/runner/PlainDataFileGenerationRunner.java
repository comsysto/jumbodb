package org.jumbodb.benchmark.generator.runner;

import org.apache.commons.io.IOUtils;

import java.io.*;


public class PlainDataFileGenerationRunner extends DataFileGenerationRunner{

    private BufferedOutputStream bos;
    private FileOutputStream fos;

    public PlainDataFileGenerationRunner(String outputFilePath, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
       super(outputFilePath, dataSetsPerFile, randomizedJSONDocs);
    }

    @Override
    public OutputStream openOutputStream(String destFilePath) throws IOException {
        fos = new FileOutputStream(destFilePath);
        bos = new BufferedOutputStream(fos);
        return bos;
    }

    @Override
    public void closeOutputStream(OutputStream outputStream) {
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(bos);
    }
}
