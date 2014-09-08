package org.jumbodb.benchmark.generator.runner;

import org.apache.commons.io.IOUtils;
import org.jumbodb.benchmark.generator.config.GenerationContext;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;

/**
 * @author Ulf Gitschthaler
 */
public class SnappyV1DataFileGenerationRunner extends PlainDataFileGenerationRunner {

    private SnappyOutputStream sos;

    public SnappyV1DataFileGenerationRunner(String outputFilePath, GenerationContext context, byte[][] randomizedJSONDocs) {

        super(outputFilePath, context, randomizedJSONDocs);
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
