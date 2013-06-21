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
        for (IndexStrategy strategy : strategies) {
            strategy.onInitialize(collectionDefinition);
        }
        indexLocationsAndStrategies = buildIndexStrategies(collectionDefinition);
    }


    public String getStrategyKey(String collection, String chunkKey, String indexName) {
        return indexLocationsAndStrategies.get(new IndexKey(collection, chunkKey, indexName)).getStrategyName();
    }

    public IndexStrategy getStrategy(String collection, String chunkKey, String indexName) {
        return indexLocationsAndStrategies.get(new IndexKey(collection, chunkKey, indexName));
    }

    public IndexStrategy getStrategy(String strategyKey) {
        return buildStrategiesByName(strategies).get(strategyKey);
    }

    public void onDataChanged(CollectionDefinition collectionDefinition) {
        for (IndexStrategy strategy : strategies) {
            strategy.onDataChanged(collectionDefinition);
        }
        indexLocationsAndStrategies = buildIndexStrategies(collectionDefinition);
    }

    @Required
    public void setStrategies(List<IndexStrategy> strategies) {
        this.strategies = strategies;
    }


    protected Map<IndexKey, IndexStrategy> buildIndexStrategies(CollectionDefinition collectionDefinition){
        Map<IndexKey, IndexStrategy> result = Maps.newHashMap();
//        Map<String, IndexStrategy> strategiesByNames = buildStrategiesByName(strategies);
        for (String collectionName : collectionDefinition.getCollections()) {
            for (DeliveryChunkDefinition deliveryChunkDef : collectionDefinition.getChunks(collectionName)) {
                for (IndexDefinition indexDef : deliveryChunkDef.getIndexes()) {
                    IndexKey indexKey = new IndexKey(collectionName, deliveryChunkDef.getChunkKey(), indexDef.getName());
                    result.put(indexKey,
                            getResponsibleStrategy(indexKey));
                }
            }
        }
        return result;
    }

    private IndexStrategy getResponsibleStrategy(IndexKey indexKey) {
        for (IndexStrategy strategy : strategies) {
            if(strategy.isResponsibleFor(indexKey.getCollectionName(), indexKey.getChunkKey(), indexKey.getIndexName())) {
                return strategy;
            }
        }
        throw new IllegalStateException("Should not be reached, because NotFoundIndexStrategy should be set automatically");
    }

    private Map<String, IndexStrategy> buildStrategiesByName(List<IndexStrategy> strategies){
        Map<String, IndexStrategy> result = Maps.newHashMap();
        for (IndexStrategy strategy : strategies) {
            result.put(strategy.getStrategyName(), strategy);
        }
        return result;
    }
}
