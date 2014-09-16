package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.FieldType
import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.DataQuery
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.springframework.cache.Cache
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class JsonSnappyLineBreakRetrieveDataSetsTaskSpec extends Specification {

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
        SnappyChunksUtil.copy(new ByteArrayInputStream(data), tmpFile, data.length, 100l, 32 * 1024)
        tmpFile
    }

    def cleanupTestFiles(file) {
        new File(file.getAbsolutePath() + ".snappy.chunks").delete()
        file.delete()
    }

    def "integrated test indexed search"() {
        setup:
        def file = createTestFile()
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyLineBreakDataStrategy)
        dataStrategy.matches(_, _) >> true
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery(new IndexQuery("testIndex", QueryOperation.EQ, "somevalue"))
        when:
        def offsets = [500l, 1000l, 1500l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        def task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
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
        offsets = [0l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
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
        offsets = [50000l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
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
        offsets = [32750l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
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
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyLineBreakDataStrategy)
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100010, FieldType.VALUE))
        jumboQuery.addJsonQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100020, FieldType.VALUE))
        jumboQuery.addJsonQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100030, FieldType.VALUE))
        when:
        def task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
        def numberOfResults = task.call()
        then: "verify matching datasets "
        // implementation is a bit strange because internal spock cast from byte[] to Object[] fails
        1 * dataStrategy.matches(QueryOperation.EQ, 100010, 100010) >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 100020, 100020) >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 100030, 100030) >> true
        2994 * dataStrategy.matches(_, _, _) >> false // 1000 datasets and 3 values to check
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
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyLineBreakDataStrategy)
        def jumboQuery = new JumboQuery()
        new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
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
        new String(task.concat(startOffset, readBuffer.getBytes("UTF-8"), resultBuffer.getBytes("UTF-8"), readBuffer.size())) == expectedBuffer
        where:
        startOffset | resultBuffer       | readBuffer     | expectedBuffer
        0           | "Hello "           | "World"        | "Hello World"
        0           | "Next "            | "concatinated" | "Next concatinated"
        0           | "!abc"             | "defg!"        | "!abcdefg!"
        10          | "<removeme>Hello " | "World"        | "Hello World"
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
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyLineBreakDataStrategy)
        when:
        def jumboQuery = new JumboQuery()
        def task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
        then: "should always be true and no match call because no criteria is given"
        0 * dataStrategy.matches(_, _, _)
        task.matchingFilter('{"sample": "json"}', jumboQuery.getJsonQuery())

        when:
        jumboQuery = new JumboQuery()

        def andQuery = new DataQuery("otherfield", FieldType.FIELD, QueryOperation.EQ, "othervalue", FieldType.VALUE)
        jumboQuery.addJsonQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE, andQuery))
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
        def matching = task.matchingFilter('{"sample": "json", "otherfield": "othervalue"}', jumboQuery.getJsonQuery())
        then: "if both criterias matches matching must be true"
        matching
        1 * dataStrategy.matches(QueryOperation.EQ, _, 'json') >> true
        1 * dataStrategy.matches(QueryOperation.EQ, _, 'othervalue') >> true

        when:
        jumboQuery = new JumboQuery()

        andQuery = new DataQuery("otherfield", FieldType.FIELD, QueryOperation.EQ, "othervalue", FieldType.VALUE)
        jumboQuery.addJsonQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE, andQuery))
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
        def notMatching = !task.matchingFilter('{"sample": "json", "otherfield": "otherNEvalue"}', jumboQuery.getJsonQuery())
        then: "if one criteria does not match matching must be false"
        notMatching
        1 * dataStrategy.matches(QueryOperation.EQ, _, 'json') >> true
        1 * dataStrategy.matches(QueryOperation.EQ, _, 'otherNEvalue') >> false

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE))
        jumboQuery.addJsonQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'jsonother', FieldType.VALUE))
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
        matching = task.matchingFilter('{"sample": "json", "otherfield": "otherNEvalue"}', jumboQuery.getJsonQuery())
        then: "if one criteria in the same clause matches it must be true"
        matching
        1 * dataStrategy.matches(QueryOperation.EQ, _, 'json') >> true
        0 * dataStrategy.matches(_, _, _)

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addJsonQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'jsonother', FieldType.VALUE))
        task = new JsonSnappyLineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock)
        notMatching = !task.matchingFilter('{"sample": "json", "otherfield": "otherNEvalue"}', jumboQuery.getJsonQuery())
        then: "if no criteria matches it must be false"
        notMatching
        1 * dataStrategy.matches(QueryOperation.EQ, _, 'json') >> false
        0 * dataStrategy.matches(_, _, _)
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
}
