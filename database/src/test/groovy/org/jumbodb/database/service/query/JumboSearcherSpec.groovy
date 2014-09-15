package org.jumbodb.database.service.query

import org.apache.commons.io.FileUtils
import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.data.common.meta.CollectionProperties
import org.jumbodb.data.common.meta.IndexProperties
import org.jumbodb.database.service.configuration.JumboConfiguration
import org.jumbodb.database.service.query.data.DataStrategy
import org.jumbodb.database.service.query.data.DataStrategyManager
import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.jumbodb.database.service.query.index.IndexStrategy
import org.jumbodb.database.service.query.index.IndexStrategyManager
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager
import org.springframework.scheduling.annotation.AsyncResult
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService

/**
 * @author Carsten Hufe
 */
class JumboSearcherSpec extends Specification {

    def "initialization"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def js = createJumboSearcher()
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        when:
        js.onInitialize()
        then:
        1 * indexStrategyManagerMock.onInitialize(_)
        1 * dataStrategyManagerMock.onInitialize(_)
    }

    def "data changed"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def cacheManagerMock = Mock(CacheManager)
        def cacheMock = Mock(Cache)
        def js = createJumboSearcher()
        js.setCacheManager(cacheManagerMock)
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        when:
        js.onDataChanged()
        then:
        1 * cacheManagerMock.getCacheNames() >> ["testCache"]
        1 * cacheManagerMock.getCache("testCache") >> cacheMock
        1 * cacheMock.clear()
        1 * indexStrategyManagerMock.onDataChanged(_)
        1 * dataStrategyManagerMock.onDataChanged(_)
    }

    def "findResultAndWriteIntoCallback with index and with result"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexExecutorMock = Mock(ExecutorService)
        def chunkExecutorMock = Mock(ExecutorService)
        def dataStrategyMock = Mock(DataStrategy)
        def js = createJumboSearcher()
        def resultCallBack = Mock(ResultCallback)
        def indexQuery = new IndexQuery("testIndex", QueryOperation.EQ, "value")
        def offsets = [new FileOffset(50000, 1234l, indexQuery), new FileOffset(50000, 2345l, indexQuery)] as Set
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        js.setChunkExecutor(chunkExecutorMock)
        js.setIndexExecutor(indexExecutorMock)
        js.onInitialize()
        def query = new JumboQuery()
        query.setCollection("testCollection")
        query.addIndexQuery(indexQuery)
        when:
        def numberOfResults = js.findResultAndWriteIntoCallback(query, resultCallBack)
        then:
        numberOfResults == 2
        1 * chunkExecutorMock.submit(_ as Callable) >> { task ->
            def result = task.get(0)
            return new AsyncResult<Integer>(result.call())
        }
        1 * indexExecutorMock.submit(_ as SearchIndexTask) >> {
            return new AsyncResult<Set<FileOffset>>(offsets)
        }
        1 * dataStrategyManagerMock.getStrategy("testChunkKey", "testCollection") >> dataStrategyMock
        1 * dataStrategyMock.findDataSetsByFileOffsets(_, offsets, _, query) >> 2
    }

    def "findResultAndWriteIntoCallback without index and with result"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexExecutorMock = Mock(ExecutorService)
        def chunkExecutorMock = Mock(ExecutorService)
        def dataStrategyMock = Mock(DataStrategy)
        def js = createJumboSearcher()
        def resultCallBack = Mock(ResultCallback)
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        js.setChunkExecutor(chunkExecutorMock)
        js.setIndexExecutor(indexExecutorMock)
        js.onInitialize()
        def query = new JumboQuery()
        query.setCollection("testCollection")
        when:
        def numberOfResults = js.findResultAndWriteIntoCallback(query, resultCallBack)
        then:
        numberOfResults == 2
        1 * chunkExecutorMock.submit(_ as Callable) >> { task ->
            def result = task.get(0)
            return new AsyncResult<Integer>(result.call())
        }
        0 * indexExecutorMock.submit(_ as SearchIndexTask)
        1 * dataStrategyManagerMock.getStrategy("testChunkKey", "testCollection") >> dataStrategyMock
        1 * dataStrategyMock.findDataSetsByFileOffsets(_, [], _, query) >> 2
    }

    def "findResultAndWriteIntoCallback missing collection"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def js = createJumboSearcher()
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        js.onInitialize()
        def query = new JumboQuery()
        query.setCollection("collections_does_not_exist")
        when:
        js.findResultAndWriteIntoCallback(query, Mock(ResultCallback))
        then:
        thrown JumboCollectionMissingException
    }

    def "getDataCompressedSize"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def dataStrategyMock = Mock(DataStrategy)
        def jumboConfigMock = Mock(JumboConfiguration)
        def folder = FileUtils.getTempDirectoryPath() + "/" + UUID.randomUUID().toString() + "/"
        def dataPath = new File(folder)
        def collectionFolder = new File(folder + "testChunkKey/testVersion/testCollection/")
        FileUtils.forceMkdir(collectionFolder)
        def meta = new CollectionProperties.CollectionMeta("2012-12-12 12:12:12", "source path", "DATA_STRATEGY", "info")
        def file = new File(collectionFolder.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME)
        CollectionProperties.write(file, meta)
        def js = createJumboSearcher()
        js.setJumboConfiguration(jumboConfigMock)
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        when:
        jumboConfigMock.getDataPath() >> dataPath
        1 * dataStrategyManagerMock.getStrategy("DATA_STRATEGY") >> dataStrategyMock
        1 * dataStrategyMock.getCompressedSize(collectionFolder) >> 111l
        def size = js.getDataCompressedSize("testChunkKey", "testVersion", "testCollection")
        then:
        size == 111l
        cleanup:
        dataPath.deleteDir()
    }

    def "getDataUncompressedSize"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def dataStrategyMock = Mock(DataStrategy)
        def jumboConfigMock = Mock(JumboConfiguration)
        def folder = FileUtils.getTempDirectoryPath() + "/" + UUID.randomUUID().toString() + "/"
        def dataPath = new File(folder)
        def collectionFolder = new File(folder + "testChunkKey/testVersion/testCollection/")
        FileUtils.forceMkdir(collectionFolder)
        def meta = new CollectionProperties.CollectionMeta("2012-12-12 12:12:12", "source path", "DATA_STRATEGY", "info")
        def file = new File(collectionFolder.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME)
        CollectionProperties.write(file, meta)
        def js = createJumboSearcher()
        js.setJumboConfiguration(jumboConfigMock)
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        when:
        jumboConfigMock.getDataPath() >> dataPath
        1 * dataStrategyManagerMock.getStrategy("DATA_STRATEGY") >> dataStrategyMock
        1 * dataStrategyMock.getUncompressedSize(collectionFolder) >> 111l
        def size = js.getDataUncompressedSize("testChunkKey", "testVersion", "testCollection")
        then:
        size == 111l
        cleanup:
        dataPath.deleteDir()
    }

    def "getIndexSize"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def dataStrategyManagerMock = Mock(DataStrategyManager)
        def indexStrategyMock = Mock(IndexStrategy)
        def jumboConfigMock = Mock(JumboConfiguration)
        def folder = FileUtils.getTempDirectoryPath() + "/" + UUID.randomUUID().toString() + "/"
        def indexPath = new File(folder)
        def indexFolder = new File(folder + "testChunkKey/testVersion/testCollection/indexName/")
        FileUtils.forceMkdir(indexFolder)
        def meta = new IndexProperties.IndexMeta("2012-12-12 12:12:12", "indexName", "INDEX_STRATEGY", "source fields")
        def file = new File(indexFolder.getAbsolutePath() + "/" + IndexProperties.DEFAULT_FILENAME)
        IndexProperties.write(file, meta)
        def js = createJumboSearcher()
        js.setJumboConfiguration(jumboConfigMock)
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        when:
        jumboConfigMock.getIndexPath() >> indexPath
        1 * indexStrategyManagerMock.getStrategy("INDEX_STRATEGY") >> indexStrategyMock
        1 * indexStrategyMock.getSize(indexFolder) >> 111l
        def size = js.getIndexSize("testChunkKey", "testVersion", "testCollection")
        then:
        size == 111l
        cleanup:
        indexPath.deleteDir()
    }

    def createJumboSearcher() {
       new JumboSearcher() {
           @Override
           protected CollectionDefinition getCollectionDefinition() {
               def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [new IndexDefinition("testIndex", new File("."), "INDEX_STRATEGY")], [:], "DATA_STRATEGY")]]
               return new CollectionDefinition(cdMap)
           }
       }

    }
}
