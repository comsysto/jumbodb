package org.jumbodb.benchmark.suite.offsets;

import org.jumbodb.benchmark.suite.result.BenchmarkJob;

import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public interface OffsetGenerator {
    void configure(BenchmarkJob benchmarkJob);
    List<FileOffset> getFileOffsets();
}
