package org.jumbodb.database.service.query.index.notfound

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.database.service.importer.ImportMetaFileInformation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class NotFoundIndexStrategySpec extends Specification {
    def strategy = new NotFoundIndexStrategy()

    @Unroll
    def "strategy should always be responsible because it's the last strategy #collection"() {
        expect:
        strategy.isResponsibleFor(chunk, collection, index)
        where:
        collection      | chunk          | index
        "a_collection"  | "a_chunk_key"  | "index1"
        "a_collection2" | "a_chunk_key2" | "index2"
        "a_collection3" | "a_chunk_key3" | "index3"
    }

    def "strategy name should be NOT_FOUND"() {
        when:
        def strategyName = strategy.getStrategyName()
        then:
        strategyName == "NOT_FOUND"
    }

    def "findFileOffsets should throw an exception"() {
        when:
        strategy.findFileOffsets("collection", "chunkKey", new IndexQuery(), 5, true)
        then:
        thrown IllegalStateException
    }

    def "strategy has no supported operations"() {
        expect:
        strategy.getSupportedOperations().size() == 0
    }

    def "strategy has not data, so size is zero"() {
        expect:
        strategy.getSize(null) == 0
    }
}