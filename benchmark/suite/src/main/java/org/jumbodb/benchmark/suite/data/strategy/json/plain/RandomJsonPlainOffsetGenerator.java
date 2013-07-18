package org.jumbodb.benchmark.suite.data.strategy.json.plain;

import org.jumbodb.benchmark.suite.offsets.FileOffset;
import org.jumbodb.benchmark.suite.offsets.OffsetGenerator;
import org.jumbodb.benchmark.suite.result.BenchmarkJob;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class RandomJsonPlainOffsetGenerator implements OffsetGenerator {
    private BenchmarkJob benchmarkJob;

    @Override
    public void configure(BenchmarkJob benchmarkJob) {
        this.benchmarkJob = benchmarkJob;
        // CARSTEN
        // retrieve dataset length

    }

    @Override
    public List<FileOffset> getFileOffsets() {
        return null;
    }
}
