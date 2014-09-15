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
        dataStrategyManager.onInitialize(new CollectionDefinition(new HashMap<String, List<DeliveryChunkDefinition>>()))
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
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> true
        dataStrategyManager.onInitialize(cd)
        when:
        def strategy = dataStrategyManager.getStrategy("testChunkKey", "testCollection")
        then:
        strategy == strategyMock
    }

    def "get strategy key by collection and chunk key"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> true
        strategyMock.getStrategyName() >> "testStrategy"
        dataStrategyManager.onInitialize(cd)
        when:
        def strategyKey = dataStrategyManager.getStrategyKey("testChunkKey", "testCollection")
        then:
        strategyKey == "testStrategy"
    }

    def "buildDataStrategies"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> true
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        def result = dataStrategyManager.buildDataStrategies(cd)
        then:
        result.get(new DataKey("testChunkKey", "testCollection")) == strategyMock
        result.size() == 1
    }

    def "getResponsibleStrategy"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        when:
        def result = dataStrategyManager.getResponsibleStrategy(new DataKey("testChunkKey", "testCollection"))
        then:
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> true
        result == strategyMock
    }

    def "initialisation"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        dataStrategyManager.onInitialize(cd)
        then:
        1 * strategyMock.onInitialize(cd)
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> true
    }

    def "initialisation no responsible strategy"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        dataStrategyManager.onInitialize(cd)
        then:
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> false
        thrown IllegalStateException
    }

    def "data changed"() {
        setup:
        def dataStrategyManager = new DataStrategyManager()
        def strategyMock = Mock(DataStrategy)
        dataStrategyManager.setStrategies([strategyMock])
        def cdMap = [testCollection: [new DeliveryChunkDefinition("testChunkKey", "testCollection", [], [:], "testStrategy")]]
        def cd = new CollectionDefinition(cdMap)
        when:
        dataStrategyManager.onDataChanged(cd)
        then:
        1 * strategyMock.onDataChanged(cd)
        1 * strategyMock.isResponsibleFor("testChunkKey", "testCollection") >> true
    }
}
