package org.jumbodb.database.service.query.index;

import org.jumbodb.database.service.query.DataDeliveryChunk;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class IndexStrategyManager {
    private List<IndexStrategy> strategies;

    public void initialize(Map<String, Collection<DataDeliveryChunk>> dataDeliveryChunks) {
        onDataChanged(dataDeliveryChunks);
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

    public void onDataChanged(Map<String, Collection<DataDeliveryChunk>> dataDeliveryChunks) {
        // make reload
    }

    @Required
    public void setStrategies(List<IndexStrategy> strategies) {
        this.strategies = strategies;
    }
}
