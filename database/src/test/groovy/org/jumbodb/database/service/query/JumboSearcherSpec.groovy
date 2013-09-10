package org.jumbodb.database.service.query

import org.jumbodb.common.query.JumboQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.data.DataStrategy
import org.jumbodb.database.service.query.data.DataStrategyManager
import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.jumbodb.database.service.query.index.IndexStrategyManager
import org.springframework.cache.Cache
import org.springframework.cache.CacheManager
import org.springframework.scheduling.annotation.AsyncResult
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

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
        def offsets = [new FileOffset(50000, 1234l, []), new FileOffset(50000, 2345l, [])] as Set
        js.setIndexStrategyManager(indexStrategyManagerMock)
        js.setDataStrategyManager(dataStrategyManagerMock)
        js.setChunkExecutor(chunkExecutorMock)
        js.setIndexExecutor(indexExecutorMock)
        js.onInitialize()
        def query = new JumboQuery()
        query.addIndexQuery("testIndex", [new QueryClause(QueryOperation.EQ, "value")])
        when:
        def numberOfResults = js.findResultAndWriteIntoCallback("testCollection", query, resultCallBack)
        then:
        numberOfResults == 2
        1 * chunkExecutorMock.submit(_ as Callable) >> { task ->
            def result = task.get(0)
            return new AsyncResult<Integer>(result.call())
        }
        1 * indexExecutorMock.submit(_ as SearchIndexTask) >> {
            return new AsyncResult<Set<FileOffset>>(offsets)
        }
        1 * dataStrategyManagerMock.getStrategy("testCollection", "testChunkKey") >> dataStrategyMock
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
        when:
        def numberOfResults = js.findResultAndWriteIntoCallback("testCollection", query, resultCallBack)
        then:
        numberOfResults == 2
        1 * chunkExecutorMock.submit(_ as Callable) >> { task ->
            def result = task.get(0)
            return new AsyncResult<Integer>(result.call())
        }
        0 * indexExecutorMock.submit(_ as SearchIndexTask)
        1 * dataStrategyManagerMock.getStrategy("testCollection", "testChunkKey") >> dataStrategyMock
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
        when:
        js.findResultAndWriteIntoCallback("collections_does_not_exist", new JumboQuery(), Mock(ResultCallback))
        then:
        thrown JumboCollectionMissingException
    }

    def createJumboSearcher() {
       new JumboSearcher() {
           @Override
           protected CollectionDefinition getCollectionDefinition() {
               def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("testIndex", new File("."), "INDEX_STRATEGY")], [:], "testStrategy")]]
               return new CollectionDefinition(cdMap)
           }
       }

    }
}
