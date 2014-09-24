package org.jumbodb.database.service.query.data.lz4

import org.jumbodb.common.query.*
import org.jumbodb.data.common.lz4.Lz4Util
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.springframework.cache.Cache
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class JsonLz4LineBreakRetrieveDataSetsTaskSpec extends Specification {

    def createTestData() {
        def data = ""
        for (i in 100000..102000) {
            data += '{"msg": "This is a sample dataset", "number": ' + i + '}\n'  // 54 chars long
        }
        data.getBytes("UTF-8")
    }

    def createTestFile() {
        def tmpFile = File.createTempFile("datalz4lb", "file")
        def data = createTestData()
        Lz4Util.copy(new ByteArrayInputStream(data), tmpFile, data.length, 2000l, 64 * 1024)
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
        def dataStrategy = Mock(JsonLz4LineBreakDataStrategy)
        dataStrategy.matches(_, _) >> true
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery(new IndexQuery("testIndex", QueryOperation.EQ, "somevalue"))
        when:
        def offsets = [540l, 1080l, 1620l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        def task = new JsonLz4LineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        def numberOfResults = task.call()
        then: "verify some grouped block loading"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100010])
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100020])
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100030])
        numberOfResults == 3

        when:
        offsets = [0l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        numberOfResults = task.call()
        then: "verify loading of the first data set"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 100000])
        numberOfResults == 1

        when:
        offsets = [108000l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        numberOfResults = task.call()
        then: "verify loading of the last data set"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 102000])
        numberOfResults == 1

        when:
        offsets = [65502l].collect { new FileOffset(123, it, new IndexQuery()) } as Set
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, offsets, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
        numberOfResults = task.call()
        then: "verify loading of a data set overlapping the lz4 block (65536 byte)"
        1 * resultCallback.writeResult([msg: "This is a sample dataset", number: 101213])
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
        def dataStrategy = Mock(JsonLz4LineBreakDataStrategy)
        resultCallback.needsMore(_) >> true
        def jumboQuery = new JumboQuery()
        jumboQuery.addDataQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100010, FieldType.VALUE))
        jumboQuery.addDataQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100020, FieldType.VALUE))
        jumboQuery.addDataQuery(new DataQuery("number", FieldType.FIELD, QueryOperation.EQ, 100030, FieldType.VALUE))
        when:
        def task = new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        def numberOfResults = task.call()
        then: "verify matching datasets "
        1 * dataStrategy.matches(QueryOperation.EQ, 100010, 100010) >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 100020, 100020) >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 100030, 100030) >> true
        5997 * dataStrategy.matches(_, _, _) >> false // 1000 datasets and 3 values to check
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
        def dataStrategy = Mock(JsonLz4LineBreakDataStrategy)
        def jumboQuery = new JumboQuery()
        new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", false)
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
        0          | 0
        1          | 12008
        2          | 25016
        3          | 35024
        4          | 50032
    }

    @Unroll
    def "matchingFilter"() {
        setup:
        def file = Mock(File)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        def resultCallback = Mock(ResultCallback)
        def dataStrategy = Mock(JsonLz4LineBreakDataStrategy)
        when:
        def jumboQuery = new JumboQuery()
        def task = new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        then: "should always be true and no match call because no criteria is given"
        0 * dataStrategy.matches(_, _, _)
        task.matchingFilter([sample: "json"], jumboQuery.getDataQuery())

        when:
        jumboQuery = new JumboQuery()

        def andQuery = new DataQuery("otherfield", FieldType.FIELD, QueryOperation.EQ, "othervalue", FieldType.VALUE)
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE, andQuery))
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        def matching = task.matchingFilter([sample: "json", "otherfield": "othervalue"], jumboQuery.getDataQuery())
        then: "if both criterias matches matching must be true"
        matching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'json') >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 'othervalue', 'othervalue') >> true

        when:
        jumboQuery = new JumboQuery()
        andQuery = new DataQuery("otherfield", FieldType.FIELD, QueryOperation.EQ, "othervalue", FieldType.VALUE)
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE, andQuery))
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        def notMatching = !task.matchingFilter([sample: "json", "otherfield": "otherNEvalue"], jumboQuery.getDataQuery())
        then: "if one criteria does not match matching must be false"
        notMatching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'json') >> true
        1 * dataStrategy.matches(QueryOperation.EQ, 'otherNEvalue', 'othervalue') >> false

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'json', FieldType.VALUE))
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'jsonother', FieldType.VALUE))
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        matching = task.matchingFilter([sample: "json", "otherfield": "otherNEvalue"], jumboQuery.getDataQuery())
        then: "if one criteria in the same clause matches it must be true"
        matching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'json') >> true
        0 * dataStrategy.matches(_, _, _) >> false

        when:
        jumboQuery = new JumboQuery()
        jumboQuery.addDataQuery(new DataQuery("sample", FieldType.FIELD, QueryOperation.EQ, 'jsonother', FieldType.VALUE))
        task = new JsonLz4LineBreakRetrieveDataSetsTask(file, [] as Set, jumboQuery, resultCallback, dataStrategy, cacheMock, cacheMock, "yyyy-MM-dd", true)
        notMatching = !task.matchingFilter([sample: "json", "otherfield": "otherNEvalue"], jumboQuery.getDataQuery())
        then: "if no criteria matches it must be false"
        notMatching
        1 * dataStrategy.matches(QueryOperation.EQ, 'json', 'jsonother') >> false
        0 * dataStrategy.matches(_, _, _) >> false
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
