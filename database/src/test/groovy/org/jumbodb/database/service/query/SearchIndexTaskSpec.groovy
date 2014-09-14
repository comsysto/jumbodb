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
        def chunkDefinition = new DeliveryChunkDefinition("testChunkKey", "testCollection", indexes, [:], "DATA_STRATEGY")
        def query = new IndexQuery("testIndex", QueryOperation.EQ, 4)
        def searchIndexTask = new SearchIndexTask(chunkDefinition, "testIndex", [query], indexStrategyManagerMock, 10, true)
        def expectedOffset = [new FileOffset(50000, 1234l, query), new FileOffset(50000, 2345l, query)] as Set
        when:
        def offsets = searchIndexTask.call()
        then:
        1 * indexStrategyManagerMock.getStrategy("testChunkKey", "testCollection", "testIndex") >> indexStrategyMock
        1 * indexStrategyMock.findFileOffsets("testChunkKey", "testCollection", "testIndex", [query], 10, true) >> expectedOffset
        expectedOffset == offsets
    }

    def "cause missing index exception"() {
        setup:
        def indexStrategyManagerMock = Mock(IndexStrategyManager)
        def indexes = [new IndexDefinition("testIndex", Mock(File), "INDEX_STRATEGY")]
        def chunkDefinition = new DeliveryChunkDefinition("testChunkKey", "testCollection", indexes, [:], "DATA_STRATEGY")
        def query = new IndexQuery("otherIndex", QueryOperation.EQ, 4)
        def searchIndexTask = new SearchIndexTask(chunkDefinition, "otherIndex", [query], indexStrategyManagerMock, 10, true)
        when:
        searchIndexTask.call()
        then:
        1 * indexStrategyManagerMock.getStrategy("testChunkKey", "testCollection", "otherIndex") >> null
        thrown JumboIndexMissingException
    }
}
