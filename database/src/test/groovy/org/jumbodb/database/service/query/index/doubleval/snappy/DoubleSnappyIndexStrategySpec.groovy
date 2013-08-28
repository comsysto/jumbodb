package org.jumbodb.database.service.query.index.doubleval.snappy

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
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

/**
 * @author Carsten Hufe
 */
class DoubleSnappyIndexStrategySpec extends Specification {
    def strategy = new DoubleSnappyIndexStrategy()

    def "verify strategy name"() {
        when:
        def strategyName = strategy.getStrategyName()
        then:
        strategyName == "DOUBLE_SNAPPY_V1"
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
        strategy.onInitialize(cd)
        def queryClause = new QueryClause(QueryOperation.EQ, 12345d)
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
        def operationMock = Mock(DoubleEqOperationSearch)
        def executorMock = Mock(ExecutorService)
        def futureMock = Mock(Future)
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        strategy.setIndexFileExecutor(executorMock)
        strategy.onInitialize(cd)
        def queryClause = new QueryClause(QueryOperation.EQ, 12345d)
        def query = new IndexQuery("testIndex", [queryClause])
        def expectedOffsets = ([12345l, 67890l] as Set)
        when:
        def fileOffsets = strategy.findFileOffsets("testCollection", "testChunkKey", query, 10)
        then:
        1 * operationMock.acceptIndexFile(_, _) >> true
        1 * futureMock.get() >> expectedOffsets
        1 * executorMock.submit(_) >> futureMock
        fileOffsets == expectedOffsets
        when:
        fileOffsets = strategy.findFileOffsets("testCollection", "testChunkKey", query, 10)
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
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFile = DoubleDataGeneration.createFile()
        DoubleDataGeneration.createIndexFile(indexFile)
        def queryClause1 = new QueryClause(QueryOperation.EQ, 1000d)
        def queryClause2 = new QueryClause(QueryOperation.EQ, 3000d)
        def queryClause3 = new QueryClause(QueryOperation.EQ, 3000.33d) // should not exist, so no result for it
        def queryClause4 = new QueryClause(QueryOperation.EQ, 5000d)
        when:
        def fileOffsets = strategy.searchOffsetsByClauses(indexFile, ([queryClause1, queryClause2, queryClause3, queryClause4] as Set), 5)
        then:
        fileOffsets == ([new FileOffset(50000, 101000l, []), new FileOffset(50000, 103000l, []), new FileOffset(50000, 105000l, [])] as Set)
        cleanup:
        indexFile.delete()
    }

    def "findOffsetForClause"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new DoubleSnappyIndexStrategy()
        def indexFile = DoubleDataGeneration.createFile()
        def snappyChunks = DoubleDataGeneration.createIndexFile(indexFile)
        def ramFile = new RandomAccessFile(indexFile, "r")
        when:
        def queryClause = new QueryClause(QueryOperation.EQ, 3333d)
        def fileOffsets = strategy.findOffsetForClause(indexFile, ramFile, queryClause, snappyChunks, 5)
        then:
        fileOffsets == ([new FileOffset(50000, 103333l, [])] as Set)
        when:
        queryClause = new QueryClause(QueryOperation.EQ, 3.3d) // should not exist, so no result for it
        fileOffsets = strategy.findOffsetForClause(indexFile, ramFile, queryClause, snappyChunks, 5)
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
        strategy.onInitialize(cd)
        when:
        def responsible = strategy.isResponsibleFor("testCollection", "testChunkKey", "testIndex")
        then:
        responsible
        when:
        def notResponsible = !strategy.isResponsibleFor("testCollection", "testChunkKey", "notIn")
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
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        when:
        strategy.onInitialize(cd)
        def ranges = strategy.buildIndexRanges()
        def testIndexFiles = ranges.get(new IndexKey("testCollection", "testChunkKey", "testIndex"))
        then:
        ranges.size() == 1
        testIndexFiles.size() == 1
        testIndexFiles[0].getIndexFile().getName() == "part00001.odx"
        testIndexFiles[0].getFrom() == -1600d
        testIndexFiles[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }

    def "createIndexFileDescription"() {
        setup:
        def indexFile = DoubleDataGeneration.createFile()
        def snappyChunks = DoubleDataGeneration.createIndexFile(indexFile)
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
        indexRange[0].getIndexFile().getName() == "part00001.odx"
        indexRange[0].getFrom() == -1600d
        indexRange[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }

    def "onImport"() {
        setup:
        def indexFileContent = DoubleDataGeneration.createIndexContent()
        def indexFolder = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString() + "/")
        def meta = new ImportMetaFileInformation(ImportMetaFileInformation.FileType.INDEX, "part00001.odx", "collection", "testindex", indexFileContent.length, "deliveryKey", "deliveryVersion", "STRATEGY")
        when:
        strategy.onImport(meta, new ByteArrayInputStream(indexFileContent), indexFolder)
        then:
        new File(indexFolder.getAbsolutePath() + "/part00001.odx").exists()
        new File(indexFolder.getAbsolutePath() + "/part00001.odx.chunks.snappy").exists()
        SnappyChunksUtil.getSnappyChunksByFile(new File(indexFolder.getAbsolutePath() + "/part00001.odx")).getLength() == indexFileContent.length
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
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testCollection", "testChunkKey", new IndexQuery("testIndex", []))
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.odx"
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
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testCollection", "testChunkKey", new IndexQuery("testIndex", []))
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.odx"
        testIndexFiles[0].getFrom() == -1600d
        testIndexFiles[0].getTo() == 15999d
        cleanup:
        indexFolder.delete()
    }

    def createIndexFolder() {
        def indexFolder = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString() + "/")
        indexFolder.mkdirs()
        DoubleDataGeneration.createIndexFile(new File(indexFolder.getAbsolutePath() + "/part00001.odx"))
        indexFolder
    }

    def createCollectionDefinition(indexFolder) {
        def index = new IndexDefinition("testIndex", indexFolder, "DOUBLE_SNAPPY_V1")
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [index], [:], "DOUBLE_SNAPPY_V1")]]
        new CollectionDefinition(cdMap)
    }


    def "acceptIndexFile operation"() {
        setup:
        def operationMock = Mock(DoubleEqOperationSearch)
        def numberSnappyIndexFile = Mock(NumberSnappyIndexFile)
        def clause = new QueryClause(QueryOperation.EQ, 5d)
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
            Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
                return [:]
            }
        }
        when:
        strategy.acceptIndexFile(new QueryClause(QueryOperation.EQ, 5d), Mock(NumberSnappyIndexFile))
        then:
        thrown UnsupportedOperationException
    }
}
