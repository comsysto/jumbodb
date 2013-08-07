package org.jumbodb.database.service.query.data

import org.jumbodb.database.service.query.definition.CollectionDefinition
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition
import spock.lang.Specification

/**
 * @author Carsten Hufe
 */
class DataStrategyManagerSpec extends Specification {

    def "get strategy by strategy key"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        strategyMock.getStrategyName() >> "testStrategy"
        dataStrategyManager.setStrategies([strategyMock])
        when:
        def strategy = dataStrategyManager.getStrategy("testStrategy")
        then:
        strategy == strategyMock
    }

    def "get strategy by collection and chunk key"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> true
        dataStrategyManager.onInitialize(cd)
        when:
        def strategy = dataStrategyManager.getStrategy("testCollection", "testChunkKey")
        then:
        strategy == strategyMock
    }

    def "get strategy key by collection and chunk key"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> true
        strategyMock.getStrategyName() >> "testStrategy"
        dataStrategyManager.onInitialize(cd)
        when:
        def strategyKey = dataStrategyManager.getStrategyKey("testCollection", "testChunkKey")
        then:
        strategyKey == "testStrategy"
    }

    def "buildDataStrategies"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> true
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        def result = dataStrategyManager.buildDataStrategies(cd)
        then:
        result.get(new DataKey("testCollection", "testChunkKey")) == strategyMock
        result.size() == 1
    }

    def "getResponsibleStrategy"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        when:
        def result = dataStrategyManager.getResponsibleStrategy(new DataKey("testCollection", "testChunkKey"))
        then:
        1 * strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> true
        result == strategyMock
    }

    def "initialisation"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        dataStrategyManager.onInitialize(cd)
        then:
        1 * strategyMock.onInitialize(cd)
        1 * strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> true
    }

    def "initialisation no responsible strategy"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        dataStrategyManager.onInitialize(cd)
        then:
        1 * strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> false
        thrown IllegalStateException
    }

    def "data changed"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testCollection", "testChunkKey", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        dataStrategyManager.onDataChanged(cd)
        then:
        1 * strategyMock.onDataChanged(cd)
        1 * strategyMock.isResponsibleFor("testCollection", "testChunkKey") >> true
    }
}
