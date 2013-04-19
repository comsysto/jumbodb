package org.jumbodb.database.service.query.index;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class IndexStrategyManager {
    private List<IndexStrategy> strategies;

    public void initialize() {
        onDataChanged();
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

    @PostConstruct
    public void onDataChanged() {
        // make reload
    }

    @Required
    public void setStrategies(List<IndexStrategy> strategies) {
        this.strategies = strategies;
    }
}
