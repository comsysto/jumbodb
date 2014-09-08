package org.jumbodb.benchmark.suite.offset.generator

import org.jumbodb.benchmark.suite.result.BenchmarkJob
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll


/**
 * @author Ulf Gitschthaler
 */
@Ignore("does not work on windows") // TODO please fix
class RandomOffsetGeneratorSpec extends Specification {

    def "fill array with gap sizes"(){
        setup:
        def generator = new RandomOffsetGenerator()
        expect:
        generator.fillWithGaps(gapArray, maxGapSize)
        Arrays.asList(gapArray).every{e -> e >= 0 && e <= maxGapSize}
        where:
        gapArray       | maxGapSize
        new long[1000] | 1
        new long[1000] | 20
    }


    @Unroll("gaps #gapSizes should be adapted to overall size of #overallSize")
    def "distribute deviation"(){
        setup:
        def generator = new RandomOffsetGenerator()
        def gapSizesCopy = new long[gapSizes.length]
        System.arraycopy(gapSizes, 0, gapSizesCopy, 0, gapSizes.length)
        when:
        generator.distributeDeviation(gapSizesCopy, overallSize)
        then:
        overallSize == Arrays.asList(gapSizesCopy).sum();
        where:
        gapSizes                   | overallSize
        [45, 20, 320, 2] as long[] | 400
        [45, 20, 320, 2] as long[] | 200
        [1] as long[]              | 50
    }

    @Unroll("gaps #gapSizes and #overallSize overall size should result in a deviation of #expDeviation")
    def "calculate deviation"(){
        setup:
        def generator = new RandomOffsetGenerator()
        expect:
        expDeviation == generator.calculateDeviation(gapSizes, overallSize)
        where:
        gapSizes                   | overallSize | expDeviation
        [] as long[]               | 20          | 20
        [1] as long[]              | 20          | 19
        [45, 20, 320, 2] as long[] | 400         | 13
        [45, 20, 320, 2] as long[] | 200         | -187
    }

    def "calculate remaining bytes for gaps"(){
        setup:
        def generator = new RandomOffsetGenerator()
        generator.configure(new BenchmarkJob(new File(""), null, null, null, null, -1, -1, -1, avgDataSetSize))
        def fakeDataFile = Mock(File.class)
        when:
        def remainingBytes = generator.calculateRemainingBytes(fakeDataFile, samplesPerFile)
        then:
        expRemainingBytes == remainingBytes
        1 * fakeDataFile.length() >> dataFileSize
        0 * _._
        where:
        dataFileSize | samplesPerFile | avgDataSetSize | expRemainingBytes
        1000000      | 1000           | 100            | 900000
        1000000      | 1              | 999999         | 1
        1000000      | 2              | 500000         | 0
    }

    @Unroll("overall gap size of #summedUpGapSize and #samplesPerFile samples should result in #expAvgGapSize average gap size")
    def "calculate average gap size"(){
        setup:
        def generator = new RandomOffsetGenerator()
        expect:
        expAvgGapSize == generator.calculateAverageGapSize(summedUpGapSize, samplesPerFile)
        where:
        summedUpGapSize | samplesPerFile | expAvgGapSize
        2000            | 1000           | 1
        1500000         | 1              | 750000
        3600000         | 2              | 1200000
        5000            | 999            | 5

    }

    @Unroll("#averageGapSize gaps should result in #expRange random number range")
    def "calculate random number range"(){
        setup:
        def generator = new RandomOffsetGenerator()
        expect:
        expRange == generator.calculateRandomNumberRange(averageGapSize)
        where:
        averageGapSize | expRange
        0              | 0
        1              | 2
        2              | 4
        500000         | 1000000
    }

    @Unroll("#nrOfRanges ranges should result in #expNrOfGaps gaps")
    def "calculate number of gaps"(){
        setup:
        def generator = new RandomOffsetGenerator()
        expect:
        expNrOfGaps == generator.calculateNumberOfGaps(nrOfRanges)
        where:
        nrOfRanges | expNrOfGaps
        0          | 0
        1          | 2
        2          | 3
        10         | 11
        5000       | 5001
    }

    @Unroll("should read length of #expFileSize bytes")
    def "read file size"(){
        setup:
        def generator = new RandomOffsetGenerator()
        def fakeDataFile = Mock(File.class)
        when:
        def size = generator.readFileSize(fakeDataFile)
        then:
        size == expFileSize
        1 * fakeDataFile.length() >> expFileSize
        0 * _._
        where:
        expFileSize << [0, 5, 10, 20]
    }
}