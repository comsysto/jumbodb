package org.jumbodb.database.service.query;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.database.service.query.index.IndexStrategyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

public class SearchIndexTask implements Callable<Set<FileOffset>> {
    private Logger log = LoggerFactory.getLogger(SearchIndexTask.class);

    private IndexStrategyManager indexStrategyManager;

    private final DeliveryChunkDefinition deliveryChunkDefinition;
    private IndexQuery query;

    public SearchIndexTask(DeliveryChunkDefinition deliveryChunkDefinition, IndexQuery query, IndexStrategyManager indexStrategyManager) {
        this.deliveryChunkDefinition = deliveryChunkDefinition;
        this.query = query;
        this.indexStrategyManager = indexStrategyManager;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        long start = System.currentTimeMillis();
        // lookup strategy
        String collection = deliveryChunkDefinition.getCollection();
        String chunkKey = deliveryChunkDefinition.getChunkKey();
        IndexStrategy strategy = indexStrategyManager.getStrategy(collection, chunkKey, query.getName());
        Set<FileOffset> fileOffsets = strategy.findFileOffsets(collection, chunkKey, query);
        log.info("Time for search one complete index offsets " + query.getClauses().size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return fileOffsets;
    }



}