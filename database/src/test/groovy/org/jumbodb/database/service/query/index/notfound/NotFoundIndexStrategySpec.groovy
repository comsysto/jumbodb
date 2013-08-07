package org.jumbodb.database.service.query.index.notfound

import org.jumbodb.common.query.IndexQuery
import org.jumbodb.common.query.JumboQuery
import org.jumbodb.database.service.importer.ImportMetaFileInformation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.jumbodb.database.service.query.data.notfound.NotFoundDataStrategy
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
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
        strategy.isResponsibleFor(collection, chunk, index)
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
        strategy.findFileOffsets("collection", "chunkKey", new IndexQuery(), 5)
        then:
        thrown IllegalStateException
    }

    def "onImport should throw an exception"() {
        when:
        strategy.onImport(Mock(ImportMetaFileInformation), Mock(InputStream), Mock(File))
        then:
        thrown UnsupportedOperationException
    }

    def "strategy has no supported operations"() {
        expect:
        strategy.getSupportedOperations().size() == 0
    }
}