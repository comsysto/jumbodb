package org.jumbodb.benchmark.suite.data.strategy.json.plain;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author Carsten Hufe
 */
public class JsonPlainDataStrategyTask implements Callable<JsonPlainDataStrategyResult> {
    private File file;
    private Set<Long> offsets;

    public JsonPlainDataStrategyTask(File file, Set<Long> offsets) {
        this.file = file;
        this.offsets = offsets;
    }

    @Override
    public JsonPlainDataStrategyResult call() throws Exception {



        // TODO
        return null;
    }
}
