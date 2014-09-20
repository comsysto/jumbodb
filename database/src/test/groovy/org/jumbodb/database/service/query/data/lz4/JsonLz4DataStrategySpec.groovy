package org.jumbodb.database.service.query.data.lz4

import org.apache.commons.io.FileUtils
import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.jumbodb.database.service.query.data.common.DataOperationSearch
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
class JsonLz4DataStrategySpec extends Specification {
    def strategy = new JsonLz4DataStrategy()

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
        def dcd = new DeliveryChunkDefinition("chunk", "collection", "yyyy-MM-dd", [], [:], "JSON_LZ4")
        strategy.onInitialize(cd)
        cd.getChunk("collection", "chunk") >> dcd
        expect:
        strategy.isResponsibleFor("chunk", "collection")
    }

    def "should not responsible, because different strategy name"() {
        setup:
        def cd = Mock(CollectionDefinition)
        def dcd = new DeliveryChunkDefinition("chunk", "collection", "yyyy-MM-dd", [], [:], "WHATEVER_STRATEGY")
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
        strategy.getStrategyName() == "JSON_LZ4"
    }

    def "matches should delegate to the appropriate operation"() {
        setup:
        def eqOperation = Mock(DataOperationSearch)
        def gtOperation = Mock(DataOperationSearch)
        def ltOperation = Mock(DataOperationSearch)
        def customStrategy = new JsonLz4DataStrategy() {
            @Override
            public Map<QueryOperation, DataOperationSearch> getOperations() {
                def ops = new HashMap<QueryOperation, DataOperationSearch>()
                ops.put(QueryOperation.EQ, eqOperation)
                ops.put(QueryOperation.GT, gtOperation)
                ops.put(QueryOperation.LT, ltOperation)
                return ops
            }
        }
        when:
        customStrategy.matches(QueryOperation.EQ, "testValue", "testValue")
        then:
        1 * eqOperation.matches(_, "testValue")
        when:
        customStrategy.matches(QueryOperation.GT, "testValue", "testValue")
        then:
        1 * gtOperation.matches(_, "testValue")
        when:
        customStrategy.matches(QueryOperation.LT, "testValue", "testValue")
        then:
        1 * ltOperation.matches(_, "testValue")
        when:
        customStrategy.matches(QueryOperation.BETWEEN, "testValue", "testValue")
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
        def customStrategy = new JsonLz4DataStrategy()
        customStrategy.setRetrieveDataExecutor(executorService)
        File mockFile = Mock()
        def indexes = [new IndexDefinition("myindex", mockFile, JsonLz4DataStrategy.JSON_LZ4)]
        def dataFiles = new HashMap<Integer, File>();
        dataFiles.put(1, mockFile)
        dataFiles.put(2, mockFile)
        dataFiles.put(3, mockFile)
        def deliveryChunkDefinition = new DeliveryChunkDefinition("testchunkkey", "testcollection", "yyyy-MM-dd", indexes, dataFiles, JsonLz4DataStrategy.JSON_LZ4)
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
        2 * executorService.submit(_ as JsonLz4RetrieveDataSetsTask) >> futureMock
        2 * futureMock.get() >> 3
    }

    def "findDataSetsByFileOffsets should run scanned search and submit 3 tasks, because no index and 3 files are existent"() {
        setup:
        def futureMock = Mock(Future)
        def executorService = Mock(ExecutorService)
        def customStrategy = new JsonLz4DataStrategy()
        customStrategy.setRetrieveDataExecutor(executorService)
        File mockFile = Mock()
        def dataFiles = new HashMap<Integer, File>();
        dataFiles.put(1, mockFile)
        dataFiles.put(2, mockFile)
        dataFiles.put(3, mockFile)
        def deliveryChunkDefinition = new DeliveryChunkDefinition("testchunkkey", "testcollection", "yyyy-MM-dd", [], dataFiles, JsonLz4DataStrategy.JSON_LZ4)
        def jumboQuery = new JumboQuery()
        def resultCallback = Mock(ResultCallback)
        when:
        def numberOfResults = customStrategy.findDataSetsByFileOffsets(deliveryChunkDefinition, [], resultCallback, jumboQuery)
        then:
        numberOfResults == 9
        3 * executorService.submit(_ as JsonLz4RetrieveDataSetsTask) >> futureMock
        3 * futureMock.get() >> 3
    }

    def "getCollectionDataSize"() {
        setup:
        def folderStr = FileUtils.getTempDirectory().absolutePath + "/" + UUID.randomUUID().toString() + "/"
        def folder = new File(folderStr)
        FileUtils.forceMkdir(folder);
        new File(folderStr + "/testdata").text = "Hello World"
        def out = new DataOutputStream(new FileOutputStream(new File(folderStr + "/testdata.blocks")))
        out.writeLong(1000l)
        out.writeLong(2000l)
        out.close()
        def strategy = new JsonLz4DataStrategy()
        when:
        def sizes = strategy.getCollectionDataSize(folder)
        then:
        sizes.getCompressedSize() == 27
        sizes.getUncompressedSize() == 1000l
        sizes.getDatasets() == 2000l
        cleanup:
        folder.deleteDir()
    }
}