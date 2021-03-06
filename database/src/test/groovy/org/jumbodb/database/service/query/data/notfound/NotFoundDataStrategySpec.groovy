package org.jumbodb.database.service.query.data.notfound

import org.jumbodb.common.query.JumboQuery
import org.jumbodb.database.service.importer.ImportMetaFileInformation
import org.jumbodb.database.service.query.FileOffset
import org.jumbodb.database.service.query.ResultCallback
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class NotFoundDataStrategySpec extends Specification {
    def strategy = new NotFoundDataStrategy()

    def "strategy should always be responsible because it's the last strategy"() {
        expect:
        strategy.isResponsibleFor(collection, chunk);
        where:
        collection      | chunk
        "a_collection"  | "a_chunk_key"
        "a_collection2" | "a_chunk_key2"
        "a_collection3" | "a_chunk_key3"
    }

    def "strategy name should be NOT_FOUND"() {
        when:
        def strategyName = strategy.getStrategyName()
        then:
        strategyName == "NOT_FOUND"
    }

    def "findDataSetsByFileOffsets should throw an exception"() {
        when:
        strategy.findDataSetsByFileOffsets(Mock(DeliveryChunkDefinition), new ArrayList<FileOffset>(), Mock(ResultCallback), Mock(JumboQuery))
        then:
        thrown IllegalStateException
    }

    def "strategy has no supported operations"() {
        expect:
        strategy.getSupportedOperations().size() == 0
    }

    def "compressed size is always zero"() {
        expect:
        strategy.getCompressedSize() == 0
    }

    def "uncompressed size is always zero"() {
        expect:
        strategy.getUncompressedSize() == 0
    }
}
