package org.jumbodb.benchmark.generator.runner;

import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Ulf Gitschthaler
 */
public class SnappyV1DataFileGenerationRunner extends PlainDataFileGenerationRunner {

    private SnappyOutputStream sos;

    public SnappyV1DataFileGenerationRunner(String outputFilePath, int dataSetsPerFile, byte[][] randomizedJSONDocs) {
        super(outputFilePath, dataSetsPerFile, randomizedJSONDocs);
    }

    @Override
    public OutputStream openOutputStream(String destFilePath) throws IOException {
        OutputStream outputStream = super.openOutputStream(destFilePath);
        sos = new SnappyOutputStream(outputStream);
        return sos;
    }

    @Override
    public void closeOutputStream(OutputStream outputStream) {
        super.closeOutputStream(outputStream);
        IOUtils.closeQuietly(sos);
    }
}
