package org.jumbodb.benchmark.suite.data.strategy.json.plain;

import com.google.common.collect.HashMultimap;
import org.jumbodb.benchmark.suite.data.strategy.DataStrategy;
import org.jumbodb.benchmark.suite.offsets.FileOffset;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;
import org.jumbodb.benchmark.suite.result.BenchmarkJobResult;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class JsonPlainDataStrategy implements DataStrategy {

    public static final String STRATEGY_NAME = "JSON_PLAIN_V1";
    private ExecutorService executorService;
    private List<FileOffset> fileOffsets;
    private BenchmarkJob benchmarkJob;

    @Override
    public void configure(List<FileOffset> fileOffsets, BenchmarkJob benchmarkJob) {
        this.fileOffsets = fileOffsets;
        this.benchmarkJob = benchmarkJob;
        executorService = Executors.newFixedThreadPool(benchmarkJob.getNumberOfThreads());
    }

    @Override
    public BenchmarkJobResult execute() {
        List<Future<JsonPlainDataStrategyResult>> results = new LinkedList<Future<JsonPlainDataStrategyResult>>();
        HashMultimap<File, Long> groupOffsetsByFile = groupOffsetsByFile(fileOffsets);
        for (File file : groupOffsetsByFile.keySet()) {
            results.add(executorService.submit(new JsonPlainDataStrategyTask(file, groupOffsetsByFile.get(file))));
        }
        // TODO

        return null;
    }

    private HashMultimap<File, Long> groupOffsetsByFile(List<FileOffset> fileOffsets) {
        // TODO
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public void cleanup() {
        executorService.shutdown();
    }

    @Override
    public String getStrategyName() {
        return STRATEGY_NAME;
    }
}
