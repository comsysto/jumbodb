package org.jumbodb.benchmark.suite.data.strategy;

import org.jumbodb.benchmark.suite.offsets.FileOffset;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;
import org.jumbodb.benchmark.suite.result.BenchmarkJobResult;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public interface DataStrategy {

    void configure(List<FileOffset> fileOffsets, BenchmarkJob benchmarkJob);

    BenchmarkJobResult execute();

    void cleanup();

    String getStrategyName();
}
