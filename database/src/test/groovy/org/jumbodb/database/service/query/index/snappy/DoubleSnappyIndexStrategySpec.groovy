package org.jumbodb.database.service.query.index.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.jumbodb.database.service.query.index.IndexKey
import org.jumbodb.database.service.query.index.common.doubleval.DoubleEqOperationSearch
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile
import org.jumbodb.database.service.query.index.common.IndexOperationSearch
import org.jumbodb.database.service.query.index.common.QueryValueRetriever
import org.springframework.cache.Cache
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

/**
 * @author Carsten Hufe
 */
class DoubleSnappyIndexStrategySpec extends Specification {
    def strategy = new DoubleSnappyIndexStrategy()


    def setup() {
        setupCache(strategy)
    }

    def setupCache(strategy) {
        def cacheMock = Mock(Cache)
        cacheMock.get(_) >> null
        strategy.setIndexCompressionBlocksCache(cacheMock)
        strategy.setIndexBlockRangesCache(cacheMock)
        strategy.setIndexQueryCache(cacheMock)
    }


    def "verify strategy name"() {
        when:
        def strategyName = strategy.getStrategyName()
        then:
        strategyName == "DOUBLE_SNAPPY"
    }

    def "verify chunk size"() {
        when:
        def snappyChunkSize = strategy.getCompressionBlockSize()
        then:
        snappyChunkSize == 32640
    }

    def "readValueFromDataInput"() {
        setup:
        def disMock = Mock(DataInput)
        when:
        def value = strategy.readValueFromDataInput(disMock)
        then:
        1 * disMock.readDouble() >> 1009d
        value == 1009d
    }

    def writeIndexEntry(value, dos) {
        dos.writeDouble(value)
        dos.writeInt(123) // file hash
        dos.writeLong(567) // offset
    }

    def "readLastValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(11111d, dos)
        writeIndexEntry(22222d, dos)
        writeIndexEntry(33333d, dos)
        writeIndexEntry(44444d, dos)
        when:
        def value = strategy.readLastValue(byteArrayStream.toByteArray())
        then:
        value == 44444d
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "readFirstValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(11111d, dos)
        writeIndexEntry(22222d, dos)
        writeIndexEntry(33333d, dos)
        writeIndexEntry(44444d, dos)
        when:
        def value = strategy.readFirstValue(byteArrayStream.toByteArray())
        then:
        value == 11111d
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "createIndexFile"() {
        setup:
        def fileMock = Mock(File)
        when:
        def indexFile = strategy.createIndexFile(123d, 456d, fileMock)
        then:
        indexFile.getFrom() == 123d
        indexFile.getTo() == 456d
        indexFile.getIndexFile() == fileMock
    }

    def "groupByIndexFile"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        setupCache(strategy)
        strategy.onInitialize(cd)
        def query = new IndexQuery("testIndex", QueryOperation.EQ, 12345d)
        when:
        def groupedByIndex = strategy.groupByIndexFile("testChunkKey", "testCollection", "testIndex", [query])
        def keys = groupedByIndex.keySet()
        def fileKey = keys.iterator().next()
        then:
        1 * operationMock.acceptIndexFile(_, _) >> true
        keys.size() == 1
        groupedByIndex.get(fileKey).get(0) == query
        when:
        groupedByIndex = strategy.groupByIndexFile("testChunkKey", "testCollection", "testIndex", [query])
        keys = groupedByIndex.keySet()
        then:
        1 * operationMock.acceptIndexFile(_, _) >> false
        keys.size() == 0
        cleanup:
        indexFolder.delete()
    }

    def "findFileOffsets"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def executorMock = Mock(ExecutorService)
        def futureMock = Mock(Future)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        setupCache(strategy)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        strategy.setIndexFileExecutor(executorMock)
        strategy.onInitialize(cd)
        def query = new IndexQuery("testIndex", QueryOperation.EQ, 12345d)
        def expectedOffsets = ([12345l, 67890l] as Set)
        when:
        def fileOffsets = strategy.findFileOffsets("testChunkKey", "testCollection", "testIndex", [query], 10, true)
        then:
        1 * operationMock.acceptIndexFile(_, _) >> true
        1 * futureMock.get() >> expectedOffsets
        1 * executorMock.submit(_) >> futureMock
        fileOffsets == expectedOffsets
        when:
        fileOffsets = strategy.findFileOffsets("testChunkKey", "testCollection", "testIndex", [query], 10, true)
        then:
        1 * operationMock.acceptIndexFile(_, _) >> false
        0 * executorMock.submit(_) >> futureMock
        fileOffsets.size() == 0
        cleanup:
        indexFolder.delete()
    }

    def "searchOffsetsByIndexQueries"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new DoubleSnappyIndexStrategy()
        setupCache(strategy)
        def indexFile = DoubleSnappyDataGeneration.createFile()
        DoubleSnappyDataGeneration.createIndexFile(indexFile)
        def query1 = new IndexQuery("testIndex", QueryOperation.EQ, 1000d)
        def query2 = new IndexQuery("testIndex", QueryOperation.EQ, 3000d)
        def query3 = new IndexQuery("testIndex", QueryOperation.EQ, 3000.33d) // should not exist, so no result for it
        def query4 = new IndexQuery("testIndex", QueryOperation.EQ, 5000d)
        when:
        def fileOffsets = strategy.searchOffsetsByIndexQueries(indexFile, ([query1, query2, query3, query4] as Set), 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 101000l, query1), new FileOffset(50000, 103000l, query2), new FileOffset(50000, 105000l, query4)] as Set)
        cleanup:
        indexFile.delete()
    }

    def "findOffsetForIndexQuery"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new DoubleSnappyIndexStrategy()
        setupCache(strategy)
        def indexFile = DoubleSnappyDataGeneration.createFile()
        def snappyChunks = DoubleSnappyDataGeneration.createIndexFile(indexFile)
        def ramFile = new RandomAccessFile(indexFile, "r")
        when:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, 3333d)
        def fileOffsets = strategy.findOffsetForIndexQuery(indexFile, ramFile, indexQuery, snappyChunks, 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 103333l, indexQuery)] as Set)
        when:
        indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, 3.3d) // should not exist, so no result for it
        fileOffsets = strategy.findOffsetForIndexQuery(indexFile, ramFile, indexQuery, snappyChunks, 5, true)
        then:
        fileOffsets.size() == 0
        cleanup:
        ramFile.close()
        indexFile.delete()
    }

    def "isResponsibleFor"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        setupCache(strategy)
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
        def operationMock = Mock(DoubleEqOperationSearch)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        setupCache(strategy)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        strategy.onInitialize(cd)
        def ranges = strategy.buildIndexRanges()
        def testIndexFiles = ranges.get(new IndexKey("testChunkKey", "testCollection", "testIndex"))
        then:
        ranges.size() == 1
        testIndexFiles.size() == 1
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -1600d
        testIndexFiles[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }

    def "createIndexFileDescription"() {
        setup:
        def indexFile = DoubleSnappyDataGeneration.createFile()
        def snappyChunks = DoubleSnappyDataGeneration.createIndexFile(indexFile)
        when:
        def indexFileDescription = strategy.createIndexFileDescription(indexFile, snappyChunks)
        then:
        indexFileDescription.getIndexFile().getName() == indexFile.getName()
        indexFileDescription.getFrom() == -1600d
        indexFileDescription.getTo() == 15999d
        cleanup:
        indexFile.delete()
    }

    def "buildIndexRange"() {
        setup:
        def indexFolder = createIndexFolder()
        when:
        def indexRange = strategy.buildIndexRange(indexFolder)
        then:
        indexRange.size() == 1
        indexRange[0].getIndexFile().getName() == "part00001.idx"
        indexRange[0].getFrom() == -1600d
        indexRange[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }

    def "onInitialize"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        setupCache(strategy)
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testChunkKey", "testCollection", "testIndex")
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -1600d
        testIndexFiles[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }


    def "onDataChanged"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        setupCache(strategy)
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testChunkKey", "testCollection", "testIndex")
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -1600d
        testIndexFiles[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }

    def createIndexFolder() {
        def indexFolder = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString() + "/")
        indexFolder.mkdirs()
        DoubleSnappyDataGeneration.createIndexFile(new File(indexFolder.getAbsolutePath() + "/part00001.idx"))
        indexFolder
    }

    def createCollectionDefinition(indexFolder) {
        def index = new IndexDefinition("testIndex", indexFolder, "DOUBLE_SNAPPY")
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", "yyyy-MM-dd", [index], [:], "DOUBLE_SNAPPY")]]
        new CollectionDefinition(cdMap)
    }


    def "acceptIndexFile operation"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def numberSnappyIndexFile = Mock(NumberIndexFile)
        def clause = new IndexQuery("testIndex", QueryOperation.EQ, 5d)
        def valueRetriever = Mock(QueryValueRetriever)
        def strategy = new DoubleSnappyIndexStrategy()
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
        def strategy = new DoubleSnappyIndexStrategy() {
            @Override
            Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> getQueryOperationsStrategies() {
                return [:]
            }
        }
        when:
        strategy.acceptIndexFile(new IndexQuery("testIndex", QueryOperation.EQ, 5d), Mock(NumberIndexFile))
        then:
        thrown UnsupportedOperationException
    }
}
