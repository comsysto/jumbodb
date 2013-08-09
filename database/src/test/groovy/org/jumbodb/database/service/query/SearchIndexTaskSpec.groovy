package org.jumbodb.database.service.query

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import org.jumbodb.database.service.query.index.IndexStrategy
import org.jumbodb.database.service.query.index.IndexStrategyManager
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class SearchIndexTaskSpec extends Specification {

    def "verify correct delegates"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def indexStrategyMock = Mock(IndexStrategy)
        def indexes = [new IndexDefinition("testIndex", Mock(File), "INDEX_STRATEGY")]
        def chunkDefinition = new DeliveryChunkDefinition("testCollection", "testChunkKey", indexes, [:], "DATA_STRATEGY")
        def query = new IndexQuery("testIndex", [new QueryClause(QueryOperation.EQ, 4)])
        def searchIndexTask = new SearchIndexTask(chunkDefinition, query, indexStrategyManagerMock, 10)
        def expectedOffset = [new FileOffset(50000, 1234l, []), new FileOffset(50000, 2345l, [])] as Set
        when:
        def offsets = searchIndexTask.call()
        then:
        1 * indexStrategyManagerMock.getStrategy("testCollection", "testChunkKey", "testIndex") >> indexStrategyMock
        1 * indexStrategyMock.findFileOffsets("testCollection", "testChunkKey", query, 10) >> expectedOffset
        expectedOffset == offsets
    }

    def "cause missing index exception"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def indexes = [new IndexDefinition("testIndex", Mock(File), "INDEX_STRATEGY")]
        def chunkDefinition = new DeliveryChunkDefinition("testCollection", "testChunkKey", indexes, [:], "DATA_STRATEGY")
        def query = new IndexQuery("otherIndex", [new QueryClause(QueryOperation.EQ, 4)])
        def searchIndexTask = new SearchIndexTask(chunkDefinition, query, indexStrategyManagerMock, 10)
        when:
        searchIndexTask.call()
        then:
        1 * indexStrategyManagerMock.getStrategy("testCollection", "testChunkKey", "otherIndex") >> null
        thrown JumboIndexMissingException
    }
}
