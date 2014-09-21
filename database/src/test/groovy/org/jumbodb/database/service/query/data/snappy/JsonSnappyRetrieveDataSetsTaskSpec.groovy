package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.*
import org.jumbodb.data.common.snappy.SnappyUtil
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.springframework.cache.Cache
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class JsonSnappyRetrieveDataSetsTaskSpec extends Specification {

    def createTestData() {
        def bos = new ByteArrayOutputStream()
        def dos = new DataOutputStream(bos)
        for (i in 100000..101000) {
            def data = '{"msg": "This is a sample dataset", "number": ' + i + '}'  // 53 chars long + 4 length
            def bytes = data.getBytes("UTF-8")
            dos.writeInt(bytes.length)
            dos.write(bytes)
        }
        dos.writeInt(-1)
        bos.toByteArray()
    }

    def createTestFile() {
        def tmpFile = File.createTempFile("data", "file")
        def data = createTestData()
        SnappyUtil.copy(new ByteArrayInputStream(data), tmpFile, data.length, 100l, 32 * 1024)
        tmpFile
    }

    def cleanupTestFiles(file) {
        new File(file.getAbsolutePath() + ".blocks").delete()
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
        def offsets = [570l, 1140l, 1710l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        def task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        def numberOfResults = task.call()
        then: "verify some grouped block loading"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100010])
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100020])
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100030])
        numberOfResults == 3

        when:
        offsets = [0l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        numberOfResults = task.call()
        then: "verify loading of the first data set"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100000])
        numberOfResults == 1

        when:
        offsets = [57000l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        numberOfResults = task.call()
        then: "verify loading of the last data set"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 101000])
        numberOfResults == 1

        when:
        offsets = [32718l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonSnappyRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        numberOfResults = task.call()
        then: "verify loading of a data set overlapping the snappy block (32768 byte)"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100574])
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
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addDataQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100010, FieldType.VALUE))
        jumboQuery.addDataQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100020, FieldType.VALUE))
        jumboQuery.addDataQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100030, FieldType.VALUE))
        when:
        def task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        def numberOfResults = task.call()
        then: "verify matching datasets "
        1 * dataStrategy.matches(QueryOperation.EQ, 100010, 100010) >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 100020, 100020) >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 100030, 100030) >> true
        2997 * dataStrategy.matches(_, _, _) >> false // 1000 datasets and 3 values to check
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100010])
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100020])
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100030])
        numberOfResults == 3
        cleanup:
        cleanupTestFiles(file)
    }

    def createDefaultTask() {
        def file = Mock(File)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        def jumboQuery = new JumboQuery()
        new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
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
    def "calculateBlockOffsetUncompressed blockIndex=#blockIndex blockSize=#blockSize == #uncompressedOffset"() {
        setup:
        def task = createDefaultTask()
        expect:
        task.calculateBlockOffsetUncompressed(blockIndex, blockSize) == uncompressedOffset
        where:
        blockIndex | blockSize | uncompressedOffset
        1          | 32000     | 32000
        2          | 32000     | 64000
        3          | 32768     | 98304
    }

    @Unroll
    def "calculateBlockOffsetCompressed blockIndex=#blockIndex == #compressedOffset"() {
        setup:
        def task = createDefaultTask()
        expect:
        def compressedBlockSizes = [12000, 13000, 10000, 15000]
        task.calculateBlockOffsetCompressed(blockIndex, compressedBlockSizes) == compressedOffset
        where:
        blockIndex | compressedOffset
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
        def dataStrategy = Mock(JsonSnappyDataStrategy)
        when:
        def jumboQuery = new JumboQuery()
        def task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        then: "should always be true and no match call because no criteria is given"
        0 * dataStrategy.matches(_, _, _)
        task.matchingFilter([sample: "json"], jumboQuery.getDataQuery())

        when:
        jumboQuery = new JumboQuery()

        def andQuery = new DataQuery("otherfield", FieldType.FIELD, QueryOperation.EQ, "othervalue", FieldType.VALUE)
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE, andQuery))
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        def matching = task.matchingFilter([sample: "json", "otherfield": "othervalue"], jumboQuery.getDataQuery())
        then: "if both criterias matches matching must be true"
        matching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'json') >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 'othervalue', 'othervalue') >> true

        when:
        jumboQuery = new JumboQuery()
        andQuery = new DataQuery("otherfield", FieldType.FIELD, QueryOperation.EQ, "othervalue", FieldType.VALUE)
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE, andQuery))
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        def notMatching = !task.matchingFilter([sample: "json", "otherfield": "otherNEvalue"], jumboQuery.getDataQuery())
        then: "if one criteria does not match matching must be false"
        notMatching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'json') >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 'otherNEvalue', 'othervalue') >> false

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE))
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'jsonother', FieldType.VALUE))
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        matching = task.matchingFilter([sample: "json", "otherfield": "otherNEvalue"], jumboQuery.getDataQuery())
        then: "if one criteria in the same clause matches it must be true"
        matching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'json') >> true
        0 * dataStrategy.matches(_, _, _) >> false

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'jsonother', FieldType.VALUE))
        task = new JsonSnappyRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        notMatching = !task.matchingFilter([sample: "json", "otherfield": "otherNEvalue"], jumboQuery.getDataQuery())
        then: "if no criteria matches it must be false"
        notMatching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'jsonother') >> false
        0 * dataStrategy.matches(_, _, _) >> false
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
}
