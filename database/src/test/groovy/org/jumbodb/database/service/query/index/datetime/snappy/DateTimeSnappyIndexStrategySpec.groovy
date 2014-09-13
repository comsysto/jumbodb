package org.jumbodb.database.service.query.index.datetime.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.jumbodb.database.service.query.index.IndexKey
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever
import org.springframework.cache.Cache
import spock.lang.Specification

import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future


/**
 * @author Carsten Hufe
 */
class DateTimeSnappyIndexStrategySpec extends Specification {
    def strategy = new DateTimeSnappyIndexStrategy()

    def setup() {
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.setIndexQueryCache(cacheMock)
    }

    def "verify strategy name"() {
        when:
        def strategyName = strategy.getStrategyName()
        then:
        strategyName == "DATETIME_SNAPPY_V1"
    }

    def "verify chunk size"() {
        when:
        def snappyChunkSize = strategy.getSnappyChunkSize()
        then:
        snappyChunkSize == 32640
    }

    def "readValueFromDataInput"() {
        setup:
        def disMock = Mock(DataInput)
        when:
        def value = strategy.readValueFromDataInput(disMock)
        then:
        1 * disMock.readLong() >> 1009l
        value == 1009l
    }

    def writeIndexEntry(value, dos) {
        dos.writeLong(value)
        dos.writeInt(123) // file hash
        dos.writeLong(567) // offset
    }

    def "readLastValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(123456789012345, dos)
        writeIndexEntry(123, dos)
        writeIndexEntry(345, dos)
        writeIndexEntry(543210987654321, dos)
        when:
        def value = strategy.readLastValue(byteArrayStream.toByteArray())
        then:
        value == 543210987654321
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "readFirstValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(123456789012345, dos)
        writeIndexEntry(123, dos)
        writeIndexEntry(345, dos)
        writeIndexEntry(543210987654321, dos)
        when:
        def value = strategy.readFirstValue(byteArrayStream.toByteArray())
        then:
        value == 123456789012345
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "createIndexFile"() {
        setup:
        def fileMock = Mock(File)
        when:
        def indexFile = strategy.createIndexFile(123, 456, fileMock)
        then:
        indexFile.getFrom() == 123
        indexFile.getTo() == 456
        indexFile.getIndexFile() == fileMock
    }

    def "groupByIndexFile"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.onInitialize(cd)
        def queryClause = new QueryClause(QueryOperation.EQ, "2012-06-29 11:34:48")
        def query = new IndexQuery("testIndex", [queryClause])

        when:
        def groupedByIndex = strategy.groupByIndexFile("testChunkKey", "testCollection", query)
        def keys = groupedByIndex.keySet()
        def fileKey = keys.iterator().next()
        then:
        1 * operationMock.acceptIndexFile(_, _) >> true
        keys.size() == 1
        groupedByIndex.get(fileKey).get(0) == queryClause
        when:
        groupedByIndex = strategy.groupByIndexFile("testChunkKey", "testCollection", query)
        keys = groupedByIndex.keySet()
        then:
        1 * operationMock.acceptIndexFile(_, _) >> false
        keys.size() == 0
        cleanup:
        indexFolder.delete()
    }

    def "findFileOffsets"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def executorMock = Mock(ExecutorService)
        def futureMock = Mock(Future)
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        strategy.setIndexFileExecutor(executorMock)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.onInitialize(cd)
        def queryClause = new QueryClause(QueryOperation.EQ, "2012-06-29 11:34:48")
        def query = new IndexQuery("testIndex", [queryClause])
        def expectedOffsets = ([12345l, 67890l] as Set)
        when:
        def fileOffsets = strategy.findFileOffsets("testCollection", "testChunkKey", query, 10, true)
        then:
        1 * operationMock.acceptIndexFile(_, _) >> true
        1 * futureMock.get() >> expectedOffsets
        1 * executorMock.submit(_) >> futureMock
        fileOffsets == expectedOffsets
        when:
        fileOffsets = strategy.findFileOffsets("testCollection", "testChunkKey", query, 10, true)
        then:
        1 * operationMock.acceptIndexFile(_, _) >> false
        0 * executorMock.submit(_) >> futureMock
        fileOffsets.size() == 0
        cleanup:
        indexFolder.delete()
    }

    def "searchOffsetsByClauses"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFile = DateTimeDataGeneration.createFile()
        DateTimeDataGeneration.createIndexFile(indexFile)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.setIndexQueryCache(cacheMock)
        def queryClause1 = new QueryClause(QueryOperation.EQ, "2012-01-01 12:00:00")
        def queryClause2 = new QueryClause(QueryOperation.EQ, "2012-05-01 12:00:00")
        def queryClause3 = new QueryClause(QueryOperation.EQ, "2012-05-01 12:00:01") // should not exist, so no result for it
        def queryClause4 = new QueryClause(QueryOperation.EQ, "2012-12-29 11:34:48")

        when:
        def fileOffsets = strategy.searchOffsetsByClauses(indexFile, ([queryClause1, queryClause2, queryClause3, queryClause4] as Set), 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 1356777388000, []), new FileOffset(50000, 1335866500000, []), new FileOffset(50000, 1325415700000, [])] as Set)
        cleanup:
        indexFile.delete()
    }

    def "findOffsetForClause"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFile = DateTimeDataGeneration.createFile()
        def snappyChunks = DateTimeDataGeneration.createIndexFile(indexFile)
        def ramFile = new RandomAccessFile(indexFile, "r")
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.setIndexQueryCache(cacheMock)
        when:
        def queryClause = new QueryClause(QueryOperation.EQ, "2012-01-01 12:00:00")
        def fileOffsets = strategy.findOffsetForClause(indexFile, ramFile, queryClause, snappyChunks, 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 1325415700000, [])] as Set)
        when:
        queryClause = new QueryClause(QueryOperation.EQ, "2012-01-01 12:00:01") // should not exist, so no result for it
        fileOffsets = strategy.findOffsetForClause(indexFile, ramFile, queryClause, snappyChunks, 5, true)
        then:
        fileOffsets.size() == 0
        cleanup:
        ramFile.close()
        indexFile.delete()
    }

    def "isResponsibleFor"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.onInitialize(cd)
        when:
        def responsible = strategy.isResponsibleFor("testChunkKey", "testCollection", "testIndex")
        then:
        responsible
        when:
        def notResponsible = !strategy.isResponsibleFor("testChunkKey", "testCollection", "notIn")
        then:
        notResponsible
        cleanup:
        indexFolder.delete()
    }

    def "buildIndexRanges"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        when:
        strategy.onInitialize(cd)
        def ranges = strategy.buildIndexRanges()
        def testIndexFiles = ranges.get(new IndexKey("testChunkKey", "testCollection", "testIndex"))
        then:
        ranges.size() == 1
        testIndexFiles.size() == 1
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        sdf.format(new Date(testIndexFiles[0].getFrom())) == "2012-01-01 12:00:00"
        sdf.format(new Date(testIndexFiles[0].getTo())) == "2012-12-29 11:34:48"
        cleanup:
        indexFolder.delete()
    }

    def "createIndexFileDescription"() {
        setup:
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        def indexFile = File.createTempFile("part00001", "odx")
        def snappyChunks = DateTimeDataGeneration.createIndexFile(indexFile)
        when:
        def indexFileDescription = strategy.createIndexFileDescription(indexFile, snappyChunks)
        then:
        indexFileDescription.getIndexFile().getName() == indexFile.getName()
        sdf.format(new Date(indexFileDescription.getFrom())) == "2012-01-01 12:00:00"
        sdf.format(new Date(indexFileDescription.getTo())) == "2012-12-29 11:34:48"
        cleanup:
        indexFile.delete()
    }

    def "buildIndexRange"() {
        setup:
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        def indexFolder = createIndexFolder()
        when:
        def indexRange = strategy.buildIndexRange(indexFolder)
        then:
        indexRange.size() == 1
        indexRange[0].getIndexFile().getName() == "part00001.idx"
        sdf.format(new Date(indexRange[0].getFrom())) == "2012-01-01 12:00:00"
        sdf.format(new Date(indexRange[0].getTo())) == "2012-12-29 11:34:48"
        cleanup:
        indexFolder.delete()
    }

    def "onInitialize"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testChunkKey", "testCollection", new IndexQuery("testIndex", []))
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        sdf.format(new Date(testIndexFiles[0].getFrom())) == "2012-01-01 12:00:00"
        sdf.format(new Date(testIndexFiles[0].getTo())) == "2012-12-29 11:34:48"
        cleanup:
        indexFolder.delete()
    }


    def "onDataChanged"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def strategy = new DateTimeSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexSnappyChunksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testChunkKey", "testCollection", new IndexQuery("testIndex", []))
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        sdf.format(new Date(testIndexFiles[0].getFrom())) == "2012-01-01 12:00:00"
        sdf.format(new Date(testIndexFiles[0].getTo())) == "2012-12-29 11:34:48"
        cleanup:
        indexFolder.delete()
    }

    def createIndexFolder() {
        def indexFolder = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString() + "/")
        indexFolder.mkdirs()
        DateTimeDataGeneration.createIndexFile(new File(indexFolder.getAbsolutePath() + "/part00001.idx"))
        indexFolder
    }

    def createCollectionDefinition(indexFolder) {
        def index = new IndexDefinition("testIndex", indexFolder, "DATETIME_SNAPPY_V1")
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [index], [:], "DATETIME_SNAPPY_V1")]]
        new CollectionDefinition(cdMap)
    }


    def "acceptIndexFile operation"() {
        setup:
        def operationMock = Mock(DateTimeEqOperationSearch)
        def numberSnappyIndexFile = Mock(NumberSnappyIndexFile)
        def clause = new QueryClause(QueryOperation.EQ, "a date")
        def valueRetriever = Mock(QueryValueRetriever)
        def strategy = new DateTimeSnappyIndexStrategy()
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        def result = strategy.acceptIndexFile(clause, numberSnappyIndexFile)
        then:
        1 * operationMock.getQueryValueRetriever(clause) >> valueRetriever
        1 * operationMock.acceptIndexFile(valueRetriever, numberSnappyIndexFile) >> true
        result == true
        when:
        result = strategy.acceptIndexFile(clause, numberSnappyIndexFile)
        then:
        1 * operationMock.getQueryValueRetriever(clause) >> valueRetriever
        1 * operationMock.acceptIndexFile(valueRetriever, numberSnappyIndexFile) >> false
        result == false
    }


    def "acceptIndexFile exception"() {
        setup:
        def strategy = new DateTimeSnappyIndexStrategy() {
            @Override
            Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
                return [:]
            }
        }
        when:
        strategy.acceptIndexFile(new QueryClause(QueryOperation.EQ, "a date"), Mock(NumberSnappyIndexFile))
        then:
        thrown UnsupportedOperationException
    }
}
