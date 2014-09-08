package org.jumbodb.benchmark.suite.offset.generator

import org.apache.commons.lang.RandomStringUtils
import org.jumbodb.benchmark.suite.offset.FileOffset
import org.jumbodb.benchmark.suite.result.BenchmarkJob
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
/**
 * @author Ulf Gitschthaler
 */
@Ignore("Does not work on Windows") // TODO please fix
class BaseOffsetGeneratorSpec extends Specification {

    @Shared
    def inputDir
    @Shared
    def collectionDir
    @Shared
    def chunkDir
    @Shared
    def dataFilesDir


    def setupSpec(){
        inputDir = new File(System.properties["java.io.tmpdir"], "data_files")
        collectionDir = new File(inputDir, "mobile_phones")
        chunkDir = new File(collectionDir, "march_delivery")
        dataFilesDir = new File(chunkDir, "1231-4325-12")
        dataFilesDir.mkdirs()

        def dataFile1 = new File(dataFilesDir, "part-r-00000")
        def dataFile2 = new File(dataFilesDir, "part-r-12314")
        def activeProperties = new File(chunkDir, "active.properties")

        dataFile1 << RandomStringUtils.random(50000)
        dataFile2 << RandomStringUtils.random(100000)

        activeProperties << String.format("#Active Delivery%n")
        activeProperties << String.format("#Tue Sep 10 14:27:21 CEST 2013%n")
        activeProperties << String.format("deliveryVersion=1231-4325-12%n")
        activeProperties << String.format("active=true")
    }

    def cleanupSpec(){
        inputDir.deleteDir()
    }

    def "generate file offsets"(){
        setup:
        def benchmarkJob = new BenchmarkJob(inputDir, null, null, "mobile_phones", "march_delivery", -1, -1, 100, -1)
        def generator = new FakeGenerator()
        generator.configure(benchmarkJob)
        when:
        generator.getFileOffsets()
        then:
        thrown(UnsupportedOperationException.class)
    }

    def "read delivery version from properties file"(){
        setup:
        def generator = new FakeGenerator()
        expect:
        "1231-4325-12" == generator.readDeliveryVersion(inputDir.absolutePath, "mobile_phones", "march_delivery")
    }

    def "build data files path"(){
        setup:
        def benchmarkJob = new BenchmarkJob(new File(inputFolder), null, null, collectionName, chunkKey, -1, -1, -1, -1)
        def generator = new FakeGenerator()
        generator.configure(benchmarkJob)
        when:
        def dataFilesPath = generator.getDataFilesPath(deliveryVersion)
        then:
        def expectedResultPath = "/opt/jumbodb/mobile_phone_locations/march_delivery/123-4444.ad"
        expectedResultPath == dataFilesPath.absolutePath
        where:
        inputFolder = "/opt/jumbodb"
        collectionName = "mobile_phone_locations"
        chunkKey = "march_delivery"
        deliveryVersion = "123-4444.ad"
    }

    @Unroll("sample calculation results in #expectedSamplesPerFile samples")
    def "calculate samples count"(){
        setup:
        def generator = new FakeGenerator()
        expect:
        expectedSamplesPerFile == generator.calculateSampleCount(nrDataFiles, nrSamples, lastfile)
        where:
        nrDataFiles | nrSamples | lastfile  | expectedSamplesPerFile
        100         | 100000    | false     | 1000
        100         | 100000    | true      | 1000
        99          | 100000    | false     | 1010
        99          | 100000    | true      | 10
    }

    private static class FakeGenerator extends BaseOffsetGenerator {
        @Override
        List<FileOffset> generateFileOffsets(File[] dataFiles, int defaultSampleCount, int lastSampleCount) {
            throw new UnsupportedOperationException("Don't use me!");
        }
    }
}