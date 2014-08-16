package org.jumbodb.database.service.query.index

import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import org.jumbodb.database.service.query.definition.IndexDefinition
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class IndexStrategyManagerSpec extends Specification {

    def "get strategy by strategy key"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        strategyMock.getStrategyName() >> "testStrategy"
        indexStrategyManager.setStrategies([strategyMock])
        when:
        def strategy = indexStrategyManager.getStrategy("testStrategy")
        then:
        strategy == strategyMock
    }

    def "get strategy by collection, chunk key and index name"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        indexStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("indexName", Mock(File), "STRATEGY")], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        strategyMock.isResponsibleFor("testChunkKey", "testCollection", "indexName") >> true
        indexStrategyManager.onInitialize(cd)
        when:
        def strategy = indexStrategyManager.getStrategy("testCollection", "testChunkKey", "indexName")
        then:
        strategy == strategyMock
    }

    def "get strategy key by collection, chunk key and index name"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        indexStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("indexName", Mock(File), "STRATEGY")], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        strategyMock.isResponsibleFor("testChunkKey", "testCollection", "indexName") >> true
        strategyMock.getStrategyName() >> "testStrategy"
        indexStrategyManager.onInitialize(cd)
        when:
        def strategyKey = indexStrategyManager.getStrategyKey("testCollection", "testChunkKey", "indexName")
        then:
        strategyKey == "testStrategy"
    }

    def "buildDataStrategies"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        strategyMock.isResponsibleFor("testChunkKey", "testCollection", "indexName") >> true
        indexStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("indexName", Mock(File), "STRATEGY")], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        def result = indexStrategyManager.buildIndexStrategies(cd)
        then:
        result.get(new IndexKey("testChunkKey", "testCollection", "indexName")) == strategyMock
        result.size() == 1
    }

    def "getResponsibleStrategy"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        indexStrategyManager.setStrategies([strategyMock])
        when:
        def result = indexStrategyManager.getResponsibleStrategy(new IndexKey("testChunkKey", "testCollection", "testIndex"))
        then:
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection", "testIndex") >> true
        result == strategyMock
    }

    def "initialisation"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        indexStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("indexName", Mock(File), "STRATEGY")], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        indexStrategyManager.onInitialize(cd)
        then:
        1 * strategyMock.onInitialize(cd)
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection", "indexName") >> true
    }

    def "initialisation no responsible strategy"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        indexStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("indexName", Mock(File), "STRATEGY")], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        indexStrategyManager.onInitialize(cd)
        then:
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection", "indexName") >> false
        thrown IllegalStateException
    }

    def "data changed"() {
        setup:
        def indexStrategyManager = new IndexStrategyManager()
        def strategyMock = Mock(IndexStrategy)
        indexStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [new IndexDefinition("indexName", Mock(File), "STRATEGY")], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        indexStrategyManager.onDataChanged(cd)
        then:
        1 * strategyMock.onDataChanged(cd)
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection", "indexName") >> true
    }
}
