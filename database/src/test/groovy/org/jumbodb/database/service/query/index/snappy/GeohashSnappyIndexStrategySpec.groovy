package org.jumbodb.database.service.query.index.snappy

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.jumbodb.database.service.query.index.IndexKey
import org.jumbodb.database.service.query.index.common.geohash.GeohashCoords
import org.jumbodb.database.service.query.index.common.geohash.GeohashWithinRangeMeterBoxOperationSearch
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
class GeohashSnappyIndexStrategySpec extends Specification {
    def strategy = new GeohashSnappyIndexStrategy()

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
        strategyName == "GEOHASH_SNAPPY"
    }

    def "verify chunk size"() {
        when:
        def snappyChunkSize = strategy.getCompressionBlockSize()
        then:
        snappyChunkSize == 48 * 1024
    }

    def "readValueFromDataInput"() {
        setup:
        def disMock = Mock(DataInput)
        when:
        def value = strategy.readValueFromDataInput(disMock)
        then:
        1 * disMock.readInt() >> 1009
        2 * disMock.readFloat() >>> [12f, 23f]
        value.getGeohash() == 1009
        value.getLatitude() == 12f
        value.getLongitude() == 23f
    }

    def writeIndexEntry(geohash, lat, lon, dos) {
        dos.writeInt(geohash)
        dos.writeFloat(lat)
        dos.writeFloat(lon)
        dos.writeInt(123) // file hash
        dos.writeLong(567) // offset
    }

    def "readLastValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(11111, 1f, 11f, dos)
        writeIndexEntry(22222, 2f, 22f, dos)
        writeIndexEntry(33333, 3f, 33f, dos)
        writeIndexEntry(44444, 4f, 44f, dos)
        when:
        def value = strategy.readLastValue(byteArrayStream.toByteArray())
        then:
        value.getGeohash() == 44444
        value.getLatitude() == 4f
        value.getLongitude() == 44f
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "readFirstValue"() {
        setup:
        def byteArrayStream = new ByteArrayOutputStream()
        def dos = new DataOutputStream(byteArrayStream)
        writeIndexEntry(11111, 1f, 11f, dos)
        writeIndexEntry(22222, 2f, 22f, dos)
        writeIndexEntry(33333, 3f, 33f, dos)
        writeIndexEntry(44444, 4f, 44f, dos)
        when:
        def value = strategy.readFirstValue(byteArrayStream.toByteArray())
        then:
        value.getGeohash() == 11111
        value.getLatitude() == 1f
        value.getLongitude() == 11f
        cleanup:
        dos.close()
        byteArrayStream.close()
    }

    def "createIndexFile"() {
        setup:
        def fileMock = Mock(File)
        when:
        def indexFile = strategy.createIndexFile(new GeohashCoords(123, 12f, 23f), new GeohashCoords(456, 45f, 56f), fileMock)
        then:
        indexFile.getFrom() == 123
        indexFile.getTo() == 456
        indexFile.getIndexFile() == fileMock
    }

    def "groupByIndexFile"() {
        setup:
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def strategy = new GeohashSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        setupCache(strategy)
        strategy.onInitialize(cd)
        def query = new IndexQuery("testIndex", QueryOperation.EQ, 12345)
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
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def executorMock = Mock(ExecutorService)
        def futureMock = Mock(Future)
        def strategy = new GeohashSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.EQ, operationMock)
        strategy.setIndexFileExecutor(executorMock)
        setupCache(strategy)
        strategy.onInitialize(cd)
        def query = new IndexQuery("testIndex", QueryOperation.EQ, 12345)
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
        def strategy = new GeohashSnappyIndexStrategy()
        setupCache(strategy)
        def indexFile = GeohashSnappyDataGeneration.createFile()
        GeohashSnappyDataGeneration.createIndexFile(indexFile)
        def query1 = new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, [[1.0, 0.01], 1])
        def query2 = new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, [[0.9, 0.01], 1]) // should not exist, so no result for it
        def query3 = new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, [[1.0, 20.48], 1000])
        when:
        def fileOffsets = strategy.searchOffsetsByIndexQueries(indexFile, ([query1, query2, query3] as Set), 1000, true)
        then:
        fileOffsets == ([new FileOffset(50000, 102047l, query1), new FileOffset(50000, 100000, query3)] as Set)
        cleanup:
        indexFile.delete()
    }

    def "findOffsetForIndexQuery"() {
        // no mocking here, instead a integrated test with equal
        setup:
        def strategy = new GeohashSnappyIndexStrategy()
        setupCache(strategy)
        def indexFile = GeohashSnappyDataGeneration.createFile()
        def snappyChunks = GeohashSnappyDataGeneration.createIndexFile(indexFile)
        def ramFile = new RandomAccessFile(indexFile, "r")
        when:
        def indexQuery = new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, [[1.0, 0.01], 1])
        def fileOffsets = strategy.findOffsetForIndexQuery(indexFile, ramFile, indexQuery, snappyChunks, 5, true)
        then:
        fileOffsets == ([new FileOffset(50000, 100000l, indexQuery)] as Set)
        when:
        indexQuery = new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, [[0.9, 0.01], 1]) // should not exist, so no result for it
        fileOffsets = strategy.findOffsetForIndexQuery(indexFile, ramFile, indexQuery, snappyChunks, 5, true)
        then:
        fileOffsets.size() == 0
        cleanup:
        ramFile.close()
        indexFile.delete()
    }

    def "isResponsibleFor"() {
        setup:
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def strategy = new GeohashSnappyIndexStrategy()
        setupCache(strategy)
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.GEO_WITHIN_RANGE_METER, operationMock)
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
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def strategy = new GeohashSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.GEO_WITHIN_RANGE_METER, operationMock)
        setupCache(strategy)
        when:
        strategy.onInitialize(cd)
        def ranges = strategy.buildIndexRanges()
        def testIndexFiles = ranges.get(new IndexKey("testChunkKey", "testCollection", "testIndex"))
        then:
        ranges.size() == 1
        testIndexFiles.size() == 1
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -1073671086
        testIndexFiles[0].getTo() == -1057192128
        cleanup:
        indexFolder.delete()
    }

    def "createIndexFileDescription"() {
        setup:
        def indexFile = GeohashSnappyDataGeneration.createFile()
        def snappyChunks = GeohashSnappyDataGeneration.createIndexFile(indexFile)
        when:
        def indexFileDescription = strategy.createIndexFileDescription(indexFile, snappyChunks)
        then:
        indexFileDescription.getIndexFile().getName() == indexFile.getName()
        indexFileDescription.getFrom() == -1073671086
        indexFileDescription.getTo() == -1057192128
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
        indexRange[0].getFrom() == -1073671086
        indexRange[0].getTo() == -1057192128
        cleanup:
        indexFolder.delete()
    }

    def "onInitialize"() {
        setup:
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def strategy = new GeohashSnappyIndexStrategy()
        def indexFolder = createIndexFolder()
        def cd = createCollectionDefinition(indexFolder)
        strategy.OPERATIONS.put(QueryOperation.GEO_WITHIN_RANGE_METER, operationMock)
        setupCache(strategy)
        when:
        strategy.onDataChanged(cd)
        def testIndexFiles = strategy.getIndexFiles("testChunkKey", "testCollection", "testIndex")
        then:
        strategy.getCollectionDefinition() == cd
        testIndexFiles[0].getIndexFile().getName() == "part00001.idx"
        testIndexFiles[0].getFrom() == -1073671086
        testIndexFiles[0].getTo() == -1057192128
        cleanup:
        indexFolder.delete()
    }


    def "onDataChanged"() {
        setup:
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def strategy = new GeohashSnappyIndexStrategy()
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
        testIndexFiles[0].getFrom() == -1073671086
        testIndexFiles[0].getTo() == -1057192128
        cleanup:
        indexFolder.delete()
    }

    def createIndexFolder() {
        def indexFolder = new File(System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID().toString() + "/")
        indexFolder.mkdirs()
        GeohashSnappyDataGeneration.createIndexFile(new File(indexFolder.getAbsolutePath() + "/part00001.idx"))
        indexFolder
    }

    def createCollectionDefinition(indexFolder) {
        def index = new IndexDefinition("testIndex", indexFolder, "GEOHASH_SNAPPY")
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", "yyyy-MM-dd", [index], [:], "GEOHASH_SNAPPY")]]
        new CollectionDefinition(cdMap)
    }


    def "acceptIndexFile operation"() {
        setup:
        def operationMock = Mock(GeohashWithinRangeMeterBoxOperationSearch)
        def numberSnappyIndexFile = Mock(NumberIndexFile)
        def indexQuery = new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, [[1f, 2f], 5])
        def valueRetriever = Mock(QueryValueRetriever)
        def strategy = new GeohashSnappyIndexStrategy()
        strategy.OPERATIONS.put(QueryOperation.GEO_WITHIN_RANGE_METER, operationMock)
        when:
        def result = strategy.acceptIndexFile(indexQuery, numberSnappyIndexFile)
        then:
        1 * operationMock.getQueryValueRetriever(indexQuery) >> valueRetriever
        1 * operationMock.acceptIndexFile(valueRetriever, numberSnappyIndexFile) >> true
        result == true
        when:
        result = strategy.acceptIndexFile(indexQuery, numberSnappyIndexFile)
        then:
        1 * operationMock.getQueryValueRetriever(indexQuery) >> valueRetriever
        1 * operationMock.acceptIndexFile(valueRetriever, numberSnappyIndexFile) >> false
        result == false
    }


    def "acceptIndexFile exception"() {
        setup:
        def strategy = new GeohashSnappyIndexStrategy() {
            @Override
            Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> getQueryOperationsStrategies() {
                return [:]
            }
        }
        when:
        strategy.acceptIndexFile(new IndexQuery("testIndex", QueryOperation.GEO_WITHIN_RANGE_METER, 5), Mock(NumberIndexFile))
        then:
        thrown UnsupportedOperationException
    }
}
