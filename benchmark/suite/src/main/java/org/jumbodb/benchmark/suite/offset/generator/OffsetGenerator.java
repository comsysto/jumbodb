package org.jumbodb.benchmark.suite.offset.generator;

import org.jumbodb.benchmark.suite.offset.FileOffset;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;

import java.io.IOException;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public interface OffsetGenerator {

    public void configure(BenchmarkJob benchmarkJob);

    public List<FileOffset> getFileOffsets() throws IOException;
}
