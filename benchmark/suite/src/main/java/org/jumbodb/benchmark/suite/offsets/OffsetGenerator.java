package org.jumbodb.benchmark.suite.offsets;

import org.jumbodb.benchmark.suite.result.BenchmarkJob;

import java.io.IOException;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public interface OffsetGenerator {
    void configure(BenchmarkJob benchmarkJob);
    List<FileOffset> getFileOffsets() throws IOException;
}
