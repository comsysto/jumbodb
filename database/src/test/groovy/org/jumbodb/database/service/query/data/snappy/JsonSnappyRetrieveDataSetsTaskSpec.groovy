package org.jumbodb.database.service.query.data.snappy

import net.minidev.json.parser.JSONParser
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class JsonSnappyRetrieveDataSetsTaskSpec extends Specification {

    def createTestData() {
        def data = ""
        for (i in 100000..101000) {
            data += '{msg: "This is a sample dataset", number: ' + i + '}\n'  // 50 chars long
        }
        data.getBytes("UTF-8")
    }

    def createTestFile() {
        def tmpFile = File.createTempFile("data", "file")
        def data = createTestData()
        SnappyChunksUtil.copy(new ByteArrayInputStream(data), tmpFile, data.length, 32 * 1024)
        tmpFile
    }

    def cleanupTestFiles(file) {
        new File(file.getAbsolutePath() + ".chunks.snappy").delete()
        file.delete()
    }

    def "integrated test indexed search"() {
        setup:
        def file = createTestFile()
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        dataStrategy.matches(_, _) >> true
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery("testIndex", Arrays.asList(new QueryClause(QueryOperation.EQ, "somevalue")))
        when:
        def offsets = [500l, 1000l, 1500l].collect { new FileOffset(123, it, []) } as Set
        def task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy)
        def numberOfResults = task.call()
        then: "verify some grouped block loading"
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        3 * resultCallback.writeResult({ it ->
            def expectedResults = [
                    "{msg: \"This is a sample dataset\", number: 100010}",
                    "{msg: \"This is a sample dataset\", number: 100020}",
                    "{msg: \"This is a sample dataset\", number: 100030}"
            ]
            def foundDataSet = new String(it, "UTF-8")
            assert expectedResults.contains(foundDataSet)
            true
        })
        numberOfResults == 3

        when:
        offsets = [0l].collect { new FileOffset(123, it, []) } as Set
        task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy)
        numberOfResults = task.call()
        then: "verify loading of the first data set"
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        1 * resultCallback.writeResult({ it ->
            def foundDataSet = new String(it, "UTF-8")
            assert "{msg: \"This is a sample dataset\", number: 100000}" == foundDataSet
            true
        })
        numberOfResults == 1

        when:
        offsets = [50000l].collect { new FileOffset(123, it, []) } as Set
        task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy)
        numberOfResults = task.call()
        then: "verify loading of the last data set"
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        1 * resultCallback.writeResult({ it ->
            def foundDataSet = new String(it, "UTF-8")
            assert "{msg: \"This is a sample dataset\", number: 101000}" == foundDataSet
            true
        })
        numberOfResults == 1

        when:
        offsets = [32750l].collect { new FileOffset(123, it, []) } as Set
        task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy)
        numberOfResults = task.call()
        then: "verify loading of a data set overlapping the snappy chunk (32768 byte)"
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        1 * resultCallback.writeResult({ it ->
            def foundDataSet = new String(it, "UTF-8")
            assert "{msg: \"This is a sample dataset\", number: 100655}" == foundDataSet
            true
        })
        numberOfResults == 1
        cleanup:
        cleanupTestFiles(file)
    }

    def "integrated test scanned search"() {
        setup:
        def file = createTestFile()
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery("number", Arrays.asList(new QueryClause(QueryOperation.EQ, 100010), new QueryClause(QueryOperation.EQ, 100020), new QueryClause(QueryOperation.EQ, 100030)))
        when:
        def task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
        def numberOfResults = task.call()
        then: "verify matching datasets "
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        1 * dataStrategy.matches(_, 100010) >> true
        1 * dataStrategy.matches(_, 100020) >> true
        1 * dataStrategy.matches(_, 100030) >> true
        2994 * dataStrategy.matches(_, _) >> false // 1000 datasets and 3 values to check
        3 * resultCallback.writeResult({ it ->
            def expectedResults = [
                    "{msg: \"This is a sample dataset\", number: 100010}",
                    "{msg: \"This is a sample dataset\", number: 100020}",
                    "{msg: \"This is a sample dataset\", number: 100030}"
            ]
            def foundDataSet = new String(it, "UTF-8")
            assert expectedResults.contains(foundDataSet)
            true
        })
        numberOfResults == 3
        cleanup:
        cleanupTestFiles(file)
    }

    def createDefaultTask() {
        def file = Mock(File)
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        def jumboQuery = new JumboQuery()
        new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
    }

    @Unroll
    def "getResultBuffer calculate buffer size lastBuffer=#lastBufferSize, toSkip=#toSkip ==  #expectedBufferSize"() {
        setup:
        def task = createDefaultTask()
        expect:
        task.getResultBuffer(new byte[lastBufferSize], toSkip).length == expectedBufferSize
        where:
        lastBufferSize | toSkip | expectedBufferSize
        1024           | 0      | 0
        0              | 0      | 0
        1024           | 50     | 0
        1024           | -50    | 50
    }

    @Unroll
    def "getResultBuffer copy rest value from buffer '#buffer' toSkip=#toSkip == '#expectedRestValue'"() {
        setup:
        def task = createDefaultTask()
        expect:
        new String(task.getResultBuffer(buffer.getBytes("UTF-8"), toSkip), "UTF-8") == expectedRestValue
        where:
        buffer                   | toSkip | expectedRestValue
        "Some Trash Hello World" | -11    | "Hello World"
        "Some Trash Hello World" | 10     | ""
        "Some Trash Hello World" | -5     | "World"
        "Some Trash Hello World" | -17    | "Trash Hello World"
    }

    @Unroll
    def "concat to byte array buffers to one expected '#expectedBuffer'"() {
        setup:
        def task = createDefaultTask()
        expect:
        new String(task.concat(readBuffer.getBytes("UTF-8"), resultBuffer.getBytes("UTF-8"), readBuffer.size())) == expectedBuffer
        where:
        startOffset | resultBuffer      | readBuffer     | expectedBuffer
        0           | "Hello "          | "World"        | "Hello World"
        0           | "Next "           | "concatinated" | "Next concatinated"
        0           | "!abc"            | "defg!"        | "!abcdefg!"
    //    10          | "<removeme>Hello " | "World"        | "Hello World"
    }

    @Unroll
    def "calculateChunkOffsetUncompressed chunkIndex=#chunkIndex chunkSize=#chunkSize == #uncompressedOffset"() {
        setup:
        def task = createDefaultTask()
        expect:
        task.calculateChunkOffsetUncompressed(chunkIndex, chunkSize) == uncompressedOffset
        where:
        chunkIndex | chunkSize | uncompressedOffset
        1          | 32000     | 32000
        2          | 32000     | 64000
        3          | 32768     | 98304
    }

    @Unroll
    def "calculateChunkOffsetCompressed chunkIndex=#chunkIndex == #compressedOffset"() {
        setup:
        def task = createDefaultTask()
        expect:
        def compressedChunkSizes = [12000, 13000, 10000, 15000]
        task.calculateChunkOffsetCompressed(chunkIndex, compressedChunkSizes) == compressedOffset
        where:
        chunkIndex | compressedOffset
        0          | 16
        1          | 12020
        2          | 25024
        3          | 35028
        4          | 50032
    }

    @Unroll
    def "matchingFilter"() {
        setup:
        def file = Mock(File)
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        def jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
        when:
        def jumboQuery = new JumboQuery()
        def task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
        then: "should always be true and no match call because no criteria is given"
        0 * dataStrategy.matches(_, _)
        task.matchingFilter('{"sample": "json"}', jsonParser, jumboQuery.getJsonQuery())

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery("sample", [new QueryClause(QueryOperation.EQ, 'json')])
        jumboQuery.addJsonQuery("otherfield", [new QueryClause(QueryOperation.EQ, 'othervalue')])
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
        def matching = task.matchingFilter('{"sample": "json", "otherfield": "othervalue"}', jsonParser, jumboQuery.getJsonQuery())
        then: "if both criterias matches matching must be true"
        matching
        1 * dataStrategy.matches(_, 'json') >> true
        1 * dataStrategy.matches(_, 'othervalue') >> true

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery("sample", [new QueryClause(QueryOperation.EQ, 'json')])
        jumboQuery.addJsonQuery("otherfield", [new QueryClause(QueryOperation.EQ, 'othervalue')])
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
        def notMatching = !task.matchingFilter('{"sample": "json", "otherfield": "otherNEvalue"}', jsonParser, jumboQuery.getJsonQuery())
        then: "if one criteria does not match matching must be false"
        notMatching
        1 * dataStrategy.matches(_, 'json') >> true
        1 * dataStrategy.matches(_, 'otherNEvalue') >> false

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery("sample", [new QueryClause(QueryOperation.EQ, 'jsonother'), new QueryClause(QueryOperation.EQ, 'json')])
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
        matching = task.matchingFilter('{"sample": "json", "otherfield": "otherNEvalue"}', jsonParser, jumboQuery.getJsonQuery())
        then: "if one criteria in the same clause matches it must be true"
        matching
        1 * dataStrategy.matches(_, 'json') >> true
        0 * dataStrategy.matches(_, _)

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery("sample", [new QueryClause(QueryOperation.EQ, 'jsonother')])
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy)
        notMatching = !task.matchingFilter('{"sample": "json", "otherfield": "otherNEvalue"}', jsonParser, jumboQuery.getJsonQuery())
        then: "if no criteria matches it must be false"
        notMatching
        1 * dataStrategy.matches(_, 'json') >> false
        0 * dataStrategy.matches(_, _)
    }

    @Unroll
    def "getDataSetFromOffsetsGroup fromOffset=#fromOffset datasetLength=#datasetLength == '#expected'"() {
        setup:
        def task = createDefaultTask()
        def buffer = "This is a test a test buffer"
        expect:
        new String(task.getDataSetFromOffsetsGroup(buffer.getBytes("UTF-8"), fromOffset, datasetLength), "UTF-8") == expected
        where:
        fromOffset | datasetLength | expected
        5          | 2             | "is"
        10         | 4             | "test"
        10         | 11            | "test a test"
    }

    @Unroll
    def "findDatasetLengthByLineBreak '#buffer' offset=#offset == #expectedLength"() {
        setup:
        def task = createDefaultTask()
        expect:
        task.findDatasetLengthByLineBreak(buffer.getBytes("UTF-8"), offset) == expectedLength
        where:
        buffer                                                   | offset | expectedLength
        "Hello world with a line break\nthis part is ignored"    | 0      | 29
        "Hello world with a line break\nthis part is ignored"    | 6      | 23
        "Hello world with a line break\nthis\npart\nis\nignored" | 6      | 23
        "Oh no the line break is missing!"                       | 6      | -1
    }

//    def "getBufferByOffsetGroup(#offsetGroup) == #expectedBufferSize"() {
//        setup:
//        def task = createDefaultTask()
//        expect:
//        def fileOffsets = offsetGroup.collect { new FileOffset(123, it, []) }
//        task.getBufferByOffsetGroup(fileOffsets).length == expectedBufferSize
//        where:
//        offsetGroup                    | expectedBufferSize
//        [4l]                           | 16384 // default buffer size
//        [1000l, 2000l, 67000l, 69999l] | 85383 // -1000 + default buffer size
//        [1000l, 69999l]                | 85383 // -1000 + default buffer size
//        [2000l, 102000l]               | 116384 // -2000 + default buffer size
//    }
    /*
     @Unroll
     def "groupOffsetsByBufferSize == #expectedGroups"() {
         setup:
         def task = createDefaultTask()
         expect:
         def fileOffsets = offsets.collect { new FileOffset(123, it, []) }
         def expectedGroupsFileOffset = expectedGroups.collect { itArray -> itArray.collect { new FileOffset(123, it, []) }}
         task.groupOffsetsByBufferSize(fileOffsets, bufferSize) == expectedGroupsFileOffset
         where:
         offsets                                             | bufferSize | expectedGroups
         [1000l, 2000l, 5000l, 6000l, 7000l, 10000l, 11000l] | 1500       | [[1000l, 2000l], [5000l, 6000l, 7000l], [10000l, 11000l]]
         [1000l, 2000l]                                      | 1500       | [[1000l, 2000l]]
         []                                                  | 1500       | []
     }

     def "groupOffsetsByBufferSize test maximum offset group"() {
         setup:
         def task = createDefaultTask()
         when:
         def offsets = (1..2500).collect { new FileOffset(123, Long.valueOf(it * 500), [])}
         def groups = task.groupOffsetsByBufferSize(offsets, 1000)
         then: "all offsets are overlapping, but limit is 1000 per group"
         groups.size() == 3
         groups[0].size() == 1000
         groups[1].size() == 1000
         groups[2].size() == 500
     }        */
}
