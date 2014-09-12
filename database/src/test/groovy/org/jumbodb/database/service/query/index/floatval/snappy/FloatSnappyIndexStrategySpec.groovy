package org.jumbodb.database.service.query.index.floatval.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.database.service.importer.ImportMetaFileInformation
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

import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

/**
 * @author Carsten Hufe
 */
class FloatSnappyIndexStrategySpec extends Specification {
    def strategy = new FloatSnappyIndexStrategy()

    def setup() {
        setupCache(strategy)
    }

    def setupCache(strategy) {
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
        strategyName == "FLOAT_SNAPPY_V1"
    }

    def "verify chunk size"() {
        when:
        def snappyChunkSize = strategy.getSnappyChunkSize()
        then:
        snappyChunkSize == 32768
    }

    def "readValueFromDataInput"() {
        setup:
        def disMock = Mock(DataInput)
        when:
        def value = strategy.readValueFromDataInput(disMock)
        then:
        1 * disMock.readFloat() >> 1009f
        value == 1009f
    }

    def writeIndexEntry(value, dos) {
        dos.writeFloat(value)
        dos.writeInt(123) // file hash
        dos.writeLong(567) // offset
    }

    def "readLastValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(11111f, dos)
        writeIndexEntry(22222f, dos)
        writeIndexEntry(33333f, dos)
        writeIndexEntry(44444f, dos)
        when:
        def value = strategy.readLastValue(byteArrayStream.toByteArray())
        then:
        value == 44444f
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "readFirstValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(11111f, dos)
        writeIndexEntry(22222f, dos)
        writeIndexEntry(33333f, dos)
        writeIndexEntry(44444f, dos)
        when:
        def value = strategy.readFirstValue(byteArrayStream.toByteArray())
        then:
        value == 11111f
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "createIndexFile"() {
        setup:
        def fileMock = Mock(File)
        when:
        def indexFile = strategy.createIndexFile(123f, 456f, fileMock)
        then:
        indexFile.getFrom() == 123f
        indexFile.getTo() == 456f
        indexFile.getIndexFile() == fileMock
    }

    def "groupByIndexFile"() {
        setup:
        def operationMock = Mock(FloatEqOperationSearch)
        def strategy = new FloatSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        setupCache(strategy)
        strategy.onInitialize(cd)
        def queryClause = new QueryClause(QueryOperation.EQ, 12345f)
        def query = new IndexQuery("testIndex", [queryClause])
        when:
        def groupedByIndex = strategy.groupByIndexFile("testCollection", "testChunkKey", query)
        def keys = groupedByIndex.keySet()
        def fileKey = keys.iterator().next()
        then:
        1 * operationMock.acceptIndexFile(_, _) >> true
        keys.size() == 1
        groupedByIndex.get(fileKey).get(0) == queryClause
        when:
        groupedByIndex = strategy.groupByIndexFile("testCollection", "testChunkKey", query)
        keys = groupedByIndex.keySet()
        then:
        1 * operationMock.acceptIndexFile(_, _) >> false
        keys.size() == 0
        cleanup:
        indexFolder.delete()
    }

    def "findFileOffsets"() {
        setup:
        def operationMock = Mock(FloatEqOperationSearch)
        def executorMock = Mock(ExecutorService)
        def futureMock = Mock(Future)
        def strategy = new FloatSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        strategy.setIndexFileExecutor(executorMock)
        setupCache(strategy)
        strategy.onInitialize(cd)
        def queryClause = new QueryClause(QueryOperation.EQ, 12345f)
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
        def strategy = new FloatSnappyIndexStrategy()
        setupCache(strategy)
        def indexFile = FloatDataGeneration.createFile()
        FloatDataGeneration.createIndexFile(indexFile)
        def queryClause1 = new QueryClause(QueryOperation.EQ, 1000f)
        def queryClause2 = new QueryClause(QueryOperation.EQ, 3000f)
        def queryClause3 = new QueryClause(QueryOperation.EQ, 3000.33f) // should not exist, so no result for it
        def queryClause4 = new QueryClause(QueryOperation.EQ, 5000f)
        when:
        def fileOffsets = strategy.searchOffsetsByClauses(indexFile, ([queryClause1, queryClause2, queryClause3, queryClause4] as Set), 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 101000l, []), new FileOffset(50000, 103000l, []), new FileOffset(50000, 105000l, [])] as Set)
        cleanup:
        indexFile.delete()
    }

    def "findOffsetForClause"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new FloatSnappyIndexStrategy()
        setupCache(strategy)
        def indexFile = FloatDataGeneration.createFile()
        def snappyChunks = FloatDataGeneration.createIndexFile(indexFile)
        def ramFile = new RandomAccessFile(indexFile, "r")
        when:
        def queryClause = new QueryClause(QueryOperation.EQ, 3333f)
        def fileOffsets = strategy.findOffsetForClause(indexFile, ramFile, queryClause, snappyChunks, 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 103333l, [])] as Set)
        when:
        queryClause = new QueryClause(QueryOperation.EQ, 3.3f) // should not exist, so no result for it
        fileOffsets = strategy.findOffsetForClause(indexFile, ramFile, queryClause, snappyChunks, 5, true)
        then:
        fileOffsets.size() == 0
        cleanup:
        ramFile.close()
        indexFile.delete()
    }

    def "isResponsibleFor"() {
        setup:
        def operationMock = Mock(FloatEqOperationSearch)
        def strategy = new FloatSnappyIndexStrategy()
        setupCache(strategy)
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
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
        def operationMock = Mock(FloatEqOperationSearch)
        def strategy = new FloatSnappyIndexStrategy()
        setupCache(strategy)
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        strategy.onInitialize(cd)
        def ranges = strategy.buildIndexRanges()
        def testIndexFiles = ranges.get(new IndexKey("testChunkKey", "testCollection", "testIndex"))
        then:
        ranges.size() == 1
        testIndexFiles.size() == 1
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -2048f
        testIndexFiles[0].getTo() == 20479f
        cleanup:
        indexFolder.delete()
    }

    def "createIndexFileDescription"() {
        setup:
        def indexFile = FloatDataGeneration.createFile()
        def snappyChunks = FloatDataGeneration.createIndexFile(indexFile)
        when:
        def indexFileDescription = strategy.createIndexFileDescription(indexFile, snappyChunks)
        then:
        indexFileDescription.getIndexFile().getName() == indexFile.getName()
        indexFileDescription.getFrom() == -2048f
        indexFileDescription.getTo() == 20479f
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
        indexRange[0].getFrom() == -2048f
        indexRange[0].getTo() == 20479f
        cleanup:
        indexFolder.delete()
    }

    def "onInitialize"() {
        setup:
        def operationMock = Mock(FloatEqOperationSearch)
        def strategy = new FloatSnappyIndexStrategy()
        setupCache(strategy)
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testCollection", "testChunkKey", new IndexQuery("testIndex", []))
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -2048f
        testIndexFiles[0].getTo() == 20479f
        cleanup:
        indexFolder.delete()
    }


    def "onDataChanged"() {
        setup:
        def operationMock = Mock(FloatEqOperationSearch)
        def strategy = new FloatSnappyIndexStrategy()
        setupCache(strategy)
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testCollection", "testChunkKey", new IndexQuery("testIndex", []))
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -2048f
        testIndexFiles[0].getTo() == 20479f
        cleanup:
        indexFolder.delete()
    }

    def createIndexFolder() {
        def indexFolder = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString() + "/")
        indexFolder.mkdirs()
        FloatDataGeneration.createIndexFile(new File(indexFolder.getAbsolutePath() + "/part00001.idx"))
        indexFolder
    }

    def createCollectionDefinition(indexFolder) {
        def index = new IndexDefinition("testIndex", indexFolder, "FLOAT_SNAPPY_V1")
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [index], [:], "FLOAT_SNAPPY_V1")]]
        new CollectionDefinition(cdMap)
    }


    def "acceptIndexFile operation"() {
        setup:
        def operationMock = Mock(FloatEqOperationSearch)
        def numberSnappyIndexFile = Mock(NumberSnappyIndexFile)
        def clause = new QueryClause(QueryOperation.EQ, 5f)
        def valueRetriever = Mock(QueryValueRetriever)
        def strategy = new FloatSnappyIndexStrategy()
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
        def strategy = new FloatSnappyIndexStrategy() {
            @Override
            Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
                return [:]
            }
        }
        when:
        strategy.acceptIndexFile(new QueryClause(QueryOperation.EQ, 5f), Mock(NumberSnappyIndexFile))
        then:
        thrown UnsupportedOperationException
    }
}
