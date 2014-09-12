package org.jumbodb.benchmark.generator.runner;

import org.jumbodb.benchmark.generator.config.GenerationContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;

/**
 * @author Ulf Gitschthaler
 */
public abstract class DataFileGenerationRunner implements Callable<Void>{

    private final String destFilePath;
    private final int dataSetsPerFile;
    private final byte[][] jsonDocs;
    private final GenerationContext context;

    public DataFileGenerationRunner(String outputFilePath, GenerationContext context, byte[][] randomizedJSONDocs) {

        this.destFilePath = outputFilePath;
        this.dataSetsPerFile = context.getDataSetsPerFile();
        this.jsonDocs = randomizedJSONDocs;
        this.context = context;
    }

    @Override
    public Void call() throws Exception {
        OutputStream outputStream = openOutputStream(destFilePath);

        try{
            for (int dataSetNo = 0, jsonDocNo = 0; dataSetNo < dataSetsPerFile; dataSetNo++){
                outputStream.write(jsonDocs[jsonDocNo]);
                jsonDocNo = jsonDocNo < jsonDocs.length ? jsonDocNo + 1 : 0;
                context.finishGeneration();
            }
        } finally {
            closeOutputStream(outputStream);
        }
        return null;
    }

    public abstract OutputStream openOutputStream(String destFilePath) throws IOException;
    public abstract void closeOutputStream(OutputStream outputStream);
}
