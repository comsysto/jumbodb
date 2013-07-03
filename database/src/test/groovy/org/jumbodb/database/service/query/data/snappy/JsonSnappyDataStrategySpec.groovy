package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.importer.ImportMetaFileInformation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.xerial.snappy.SnappyInputStream
import spock.lang.Unroll

import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

/**
 * @author Carsten Hufe
 */
class JsonSnappyDataStrategySpec extends spock.lang.Specification {
    def strategy = new JsonSnappyDataStrategy()

    @Unroll
    def "verify supported operations #operation"() {
        expect:
        strategy.getSupportedOperations().contains(operation)
        where:
        operation << QueryOperation.values()
    }

    def "should responsible always because it's the only data strategy"() {
        expect:
        strategy.isResponsibleFor("collection", "chunk")
    }

    def "verify strategy name"() {
        expect:
        strategy.getStrategyName() == "JSON_SNAPPY_V1"
    }

    def "onImport stream should be snappy compressed saved"() {
        setup:
        def strToCompare = "Some content for the file"
        def strToWrite = strToCompare + "\n"
        def byteArrayInputStream = new ByteArrayInputStream("Some content for the file".getBytes())
        def tmpTargetFile = File.createTempFile("outputFile", "snappy")
        def tmpTargetPath = tmpTargetFile.getParentFile()
        def information = new ImportMetaFileInformation(ImportMetaFileInformation.FileType.DATA, tmpTargetFile.getName(), "collection", "index name", strToWrite.size(), "deliverykey", "version", JsonSnappyDataStrategy.JSON_SNAPPY_V1)
        when:
        strategy.onImport(information, byteArrayInputStream, tmpTargetPath)
        def is = new SnappyInputStream(new FileInputStream(tmpTargetFile))
        def reader = new BufferedReader(new InputStreamReader(is))
        then:
        reader.readLine() == strToCompare
        cleanup:
        is.close()
        reader.close()
        tmpTargetFile.delete()
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
        customStrategy.matches(new QueryClause(QueryOperation.EQ, "testValue"), "testValue")
        then:
        1 * eqOperation.matches(_, "testValue")
        when:
        customStrategy.matches(new QueryClause(QueryOperation.GT, "testValue"), "testValue")
        then:
        1 * gtOperation.matches(_, "testValue")
        when:
        customStrategy.matches(new QueryClause(QueryOperation.LT, "testValue"), "testValue")
        then:
        1 * ltOperation.matches(_, "testValue")
        when:
        customStrategy.matches(new QueryClause(QueryOperation.BETWEEN, "testValue"), "testValue")
        then:
        thrown UnsupportedOperationException
    }

    def "buildFileOffsetsMap should group by filename hash"() {
        when:
        def fileOffsets = [
                new FileOffset(1, 12),
                new FileOffset(1, 13),
                new FileOffset(2, 15),
                new FileOffset(1, 14),
                new FileOffset(1, 15),
                new FileOffset(2, 17)
        ]
        def result = strategy.buildFileOffsetsMap(fileOffsets)
        then:
        result.get(1) as Set == [12, 13, 14, 15] as Set
        result.get(2) as Set == [15, 17] as Set
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
        def deliveryChunkDefinition = new DeliveryChunkDefinition("testcollection", "testchunkkey", indexes, dataFiles, JsonSnappyDataStrategy.JSON_SNAPPY_V1)
        def fileOffsets = [
                new FileOffset(1, 12),
                new FileOffset(1, 13),
                new FileOffset(2, 15),
                new FileOffset(1, 14),
                new FileOffset(1, 15),
                new FileOffset(2, 17)
        ]
        def jumboQuery = new JumboQuery()
        jumboQuery.addIndexQuery("myindex", [new QueryClause(QueryOperation.EQ, "value")])
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
        def deliveryChunkDefinition = new DeliveryChunkDefinition("testcollection", "testchunkkey", [], dataFiles, JsonSnappyDataStrategy.JSON_SNAPPY_V1)
        def jumboQuery = new JumboQuery()
        def resultCallback = Mock(ResultCallback)
        when:
        def numberOfResults = customStrategy.findDataSetsByFileOffsets(deliveryChunkDefinition, [], resultCallback, jumboQuery)
        then:
        numberOfResults == 9
        3 * executorService.submit(_ as JsonSnappyRetrieveDataSetsTask) >> futureMock
        3 * futureMock.get() >> 3
    }
}
