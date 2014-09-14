package org.jumbodb.database.service.query.data.snappy

import org.apache.commons.io.FileUtils
import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.JsonQuery
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.snappy.SnappyChunksUtil
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

/**
 * @author Carsten Hufe
 */
class JsonSnappyDataStrategySpec extends Specification {
    def strategy = new JsonSnappyDataStrategy()

    @Unroll
    def "verify supported operations #operation"() {
        expect:
        strategy.getSupportedOperations().contains(operation)
        where:
        operation << QueryOperation.values()
    }

    def "should be responsible, because collection, chunk and strategy are matching"() {
        setup:
        def cd = Mock(CollectionDefinition)
        def dcd = new DeliveryChunkDefinition("chunk", "collection", [], [:], "JSON_SNAPPY_V1")
        strategy.onInitialize(cd)
        cd.getChunk("collection", "chunk") >> dcd
        expect:
        strategy.isResponsibleFor("chunk", "collection")
    }

    def "should not responsible, because different strategy name"() {
        setup:
        def cd = Mock(CollectionDefinition)
        def dcd = new DeliveryChunkDefinition("chunk", "collection", [], [:], "WHATEVER_STRATEGY")
        strategy.onInitialize(cd)
        cd.getChunk("collection", "chunk") >> dcd
        expect:
        !strategy.isResponsibleFor("chunk", "collection")
    }

    def "should not responsible, because collection does not exist"() {
        setup:
        def cd = Mock(CollectionDefinition)
        strategy.onInitialize(cd)
        expect:
        !strategy.isResponsibleFor("chunk", "collection")
    }

    def "verify strategy name"() {
        expect:
        strategy.getStrategyName() == "JSON_SNAPPY_V1"
    }

    def "matches should delegate to the appropriate operation"() {
        setup:
        def eqOperation = Mock(JsonOperationSearch)
        def gtOperation = Mock(JsonOperationSearch)
        def ltOperation = Mock(JsonOperationSearch)
        def customStrategy = new JsonSnappyDataStrategy() {
            @Override
            public Map<QueryOperation, JsonOperationSearch> getOperations() {
                def ops = new HashMap<QueryOperation, JsonOperationSearch>()
                ops.put(QueryOperation.EQ, eqOperation)
                ops.put(QueryOperation.GT, gtOperation)
                ops.put(QueryOperation.LT, ltOperation)
                return ops
            }
        }
        when:
        customStrategy.matches(new JsonQuery("testField", QueryOperation.EQ, "testValue"), "testValue")
        then:
        1 * eqOperation.matches(_, "testValue")
        when:
        customStrategy.matches(new JsonQuery("testField", QueryOperation.GT, "testValue"), "testValue")
        then:
        1 * gtOperation.matches(_, "testValue")
        when:
        customStrategy.matches(new JsonQuery("testField", QueryOperation.LT, "testValue"), "testValue")
        then:
        1 * ltOperation.matches(_, "testValue")
        when:
        customStrategy.matches(new JsonQuery("testField", QueryOperation.BETWEEN, "testValue"), "testValue")
        then:
        thrown UnsupportedOperationException
    }

    def "buildFileOffsetsMap should group by filename hash"() {
        when:
        def fileOffsets = [
                new FileOffset(1, 12, null),
                new FileOffset(1, 13, null),
                new FileOffset(2, 15, null),
                new FileOffset(1, 14, null),
                new FileOffset(1, 15, null),
                new FileOffset(2, 17, null)
        ]
        def result = strategy.buildFileOffsetsMap(fileOffsets)
        then:
        result.get(1) as Set == [12, 13, 14, 15].collect { new FileOffset(1, it, null) } as Set
        result.get(2) as Set == [15, 17].collect { new FileOffset(2, it, null) } as Set
    }

    def "findDataSetsByFileOffsets should run index search and submit 2 tasks, because offsets are spread in 2 files"() {
        setup:
        def futureMock = Mock(Future)
        def executorService = Mock(ExecutorService)
        def customStrategy = new JsonSnappyDataStrategy()
        customStrategy.setRetrieveDataExecutor(executorService)
        File mockFile = Mock()
        def indexes = [new IndexDefinition("myindex", mockFile, JsonSnappyDataStrategy.JSON_SNAPPY_V1)]
        def dataFiles = new HashMap<Integer, File>();
        dataFiles.put(1, mockFile)
        dataFiles.put(2, mockFile)
        dataFiles.put(3, mockFile)
        def deliveryChunkDefinition = new DeliveryChunkDefinition("testchunkkey", "testcollection", indexes, dataFiles, JsonSnappyDataStrategy.JSON_SNAPPY_V1)
        def fileOffsets = [
                new FileOffset(1, 12, null),
                new FileOffset(1, 13, null),
                new FileOffset(2, 15, null),
                new FileOffset(1, 14, null),
                new FileOffset(1, 15, null),
                new FileOffset(2, 17, null)
        ]
        def jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery(new IndexQuery("myindex", QueryOperation.EQ, "value"))
        def resultCallback = Mock(ResultCallback)
        when:
        def numberOfResults = customStrategy.findDataSetsByFileOffsets(deliveryChunkDefinition, fileOffsets, resultCallback, jumboQuery)
        then:
        numberOfResults == 6
        2 * executorService.submit(_ as JsonSnappyRetrieveDataSetsTask) >> futureMock
        2 * futureMock.get() >> 3
    }

    def "findDataSetsByFileOffsets should run scanned search and submit 3 tasks, because no index and 3 files are existent"() {
        setup:
        def futureMock = Mock(Future)
        def executorService = Mock(ExecutorService)
        def customStrategy = new JsonSnappyDataStrategy()
        customStrategy.setRetrieveDataExecutor(executorService)
        File mockFile = Mock()
        def dataFiles = new HashMap<Integer, File>();
        dataFiles.put(1, mockFile)
        dataFiles.put(2, mockFile)
        dataFiles.put(3, mockFile)
        def deliveryChunkDefinition = new DeliveryChunkDefinition("testchunkkey", "testcollection", [], dataFiles, JsonSnappyDataStrategy.JSON_SNAPPY_V1)
        def jumboQuery = new JumboQuery()
        def resultCallback = Mock(ResultCallback)
        when:
        def numberOfResults = customStrategy.findDataSetsByFileOffsets(deliveryChunkDefinition, [], resultCallback, jumboQuery)
        then:
        numberOfResults == 9
        3 * executorService.submit(_ as JsonSnappyRetrieveDataSetsTask) >> futureMock
        3 * futureMock.get() >> 3
    }

    def "getCompressedSize"() {
        setup:
        def folderStr = FileUtils.getTempDirectory().absolutePath + "/" + UUID.randomUUID().toString() + "/"
        def folder = new File(folderStr)
        FileUtils.forceMkdir(folder);
        new File(folderStr + "/testdata").text = "Hello World"
        def strategy = new JsonSnappyDataStrategy()
        when:
        def size = strategy.getCompressedSize(folder)
        then:
        size == 11
        cleanup:
        folder.deleteDir()
    }


    def "getUncompressedSize"() {
        setup:
        def folderStr = FileUtils.getTempDirectory().absolutePath + "/" + UUID.randomUUID().toString() + "/"
        def folder = new File(folderStr)
        FileUtils.forceMkdir(folder);
        def dataFile = new File(folderStr + "/testdata")
        def bytes = "Hello World".getBytes("UTF-8")
        SnappyChunksUtil.copy(new ByteArrayInputStream(bytes), dataFile, bytes.length, 32 * 1024)
        def strategy = new JsonSnappyDataStrategy()
        when:
        def size = strategy.getUncompressedSize(folder)
        then:
        size == 11
        cleanup:
        folder.deleteDir()
    }
}
