package org.jumbodb.database.service.query.index;

import org.jumbodb.database.service.query.CollectionDefinition;
import org.jumbodb.database.service.query.DeliveryChunkDefinition;
import org.springframework.beans.factory.annotation.Required;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class IndexStrategyManager {
    private List<IndexStrategy> strategies;

    public void initialize(CollectionDefinition collectionDefinition) {
        onDataChanged(collectionDefinition);
    }

    public String getStrategyKey(String collection, String chunkKey, String indexName) {
        return null;
    }

    public IndexStrategy getStrategy(String collection, String chunkKey, String indexName) {
        return null;
    }

    public IndexStrategy getStrategy(String strategyKey) {
        return null;
    }

    public void onDataChanged(CollectionDefinition collectionDefinition) {
        // make reload
    }

    @Required
    public void setStrategies(List<IndexStrategy> strategies) {
        this.strategies = strategies;
    }
}
