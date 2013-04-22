package org.jumbodb.database.service.query.data;

import com.google.common.collect.Maps;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.jumbodb.database.service.query.index.IndexKey;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.springframework.beans.factory.annotation.Required;

import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class DataStrategyManager {

    private Map<DataKey, DataStrategy> dataLocationsAndStrategies;
    private List<DataStrategy> strategies;


    public void onInitialize(CollectionDefinition collectionDefinition) {
        for (DataStrategy strategy : strategies) {
            strategy.onInitialize(collectionDefinition);
        }
        dataLocationsAndStrategies = buildDataStrategies(collectionDefinition);
    }


    public String getStrategyKey(String collection, String chunkKey) {
        return dataLocationsAndStrategies.get(new DataKey(collection, chunkKey)).getStrategyName();
    }

    public DataStrategy getStrategy(String collection, String chunkKey) {
        return dataLocationsAndStrategies.get(new DataKey(collection, chunkKey));
    }

    public DataStrategy getStrategy(String strategyKey) {
        return buildStrategiesByName(strategies).get(strategyKey);
    }

    public void onDataChanged(CollectionDefinition collectionDefinition) {
        for (DataStrategy strategy : strategies) {
            strategy.onDataChanged(collectionDefinition);
        }
        dataLocationsAndStrategies = buildDataStrategies(collectionDefinition);
    }

    @Required
    public void setStrategies(List<DataStrategy> strategies) {
        this.strategies = strategies;
    }


    private Map<DataKey, DataStrategy> buildDataStrategies(CollectionDefinition collectionDefinition){
        Map<DataKey, DataStrategy> result = Maps.newHashMap();
        for (String collectionName : collectionDefinition.getCollections()) {
            for (DeliveryChunkDefinition deliveryChunkDef : collectionDefinition.getChunks(collectionName)) {
                DataKey dataKey = new DataKey(collectionName, deliveryChunkDef.getChunkKey());
                result.put(dataKey, getResponsibleStrategy(dataKey));
            }
        }
        return result;
    }

    private DataStrategy getResponsibleStrategy(DataKey dataKey) {
        for (DataStrategy strategy : strategies) {
            if(strategy.isResponsibleFor(dataKey.getCollectionName(), dataKey.getChunkKey())) {
                return strategy;
            }
        }
        throw new IllegalStateException("Should not be reached, because NotFoundIndexStrategy should be set automatically");
    }

    private Map<String, DataStrategy> buildStrategiesByName(List<DataStrategy> strategies){
        Map<String, DataStrategy> result = Maps.newHashMap();
        for (DataStrategy strategy : strategies) {
            result.put(strategy.getStrategyName(), strategy);
        }
        return result;
    }
}
