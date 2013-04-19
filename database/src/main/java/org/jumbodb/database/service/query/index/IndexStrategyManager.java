package org.jumbodb.database.service.query.index;

import com.google.common.collect.Maps;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.springframework.beans.factory.annotation.Required;

import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class IndexStrategyManager {

    private Map<IndexKey, IndexStrategy> indexLocationsAndStrategies;
    private List<IndexStrategy> strategies;


    public void onInitialize(CollectionDefinition collectionDefinition) {
        indexLocationsAndStrategies = buildIndexStrategies(collectionDefinition);
        for (IndexStrategy strategy : strategies) {
            strategy.onInitialize(collectionDefinition);
        }
    }


    public String getStrategyKey(String collection, String chunkKey, String indexName) {
        return indexLocationsAndStrategies.get(new IndexKey(indexName, chunkKey, collection)).getStrategyName();
    }

    public IndexStrategy getStrategy(String collection, String chunkKey, String indexName) {
        return indexLocationsAndStrategies.get(new IndexKey(indexName, chunkKey, collection));
    }

    public IndexStrategy getStrategy(String strategyKey) {
        return buildStratgiesByName(strategies).get(strategyKey);
    }

    public void onDataChanged(CollectionDefinition collectionDefinition) {
        indexLocationsAndStrategies = buildIndexStrategies(collectionDefinition);
        for (IndexStrategy strategy : strategies) {
            strategy.onDataChanged(collectionDefinition);
        }
    }

    @Required
    public void setStrategies(List<IndexStrategy> strategies) {
        this.strategies = strategies;
    }


    private Map<IndexKey, IndexStrategy> buildIndexStrategies(CollectionDefinition collectionDefinition){
        Map<IndexKey, IndexStrategy> result = Maps.newHashMap();
        Map<String, IndexStrategy> strategiesByNames = buildStratgiesByName(strategies);
        for (String collectionName : collectionDefinition.getCollections()) {
            for (DeliveryChunkDefinition deliveryChunkDef : collectionDefinition.getChunks(collectionName)) {
                for (IndexDefinition indexDef : deliveryChunkDef.getIndexes()) {
                    result.put(new IndexKey(indexDef.getName(), deliveryChunkDef.getChunkKey(), collectionName),
                            strategiesByNames.get(indexDef.getStrategy()));
                }
            }
        }
        return result;
    }

    private Map<String, IndexStrategy> buildStratgiesByName(List<IndexStrategy> strategies){
        Map<String, IndexStrategy> result = Maps.newHashMap();
        for (IndexStrategy strategy : strategies) {
            result.put(strategy.getStrategyName(), strategy);
        }
        return result;
    }
}
