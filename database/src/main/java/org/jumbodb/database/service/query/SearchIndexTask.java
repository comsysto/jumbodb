package org.jumbodb.database.service.query;

import org.jumbodb.common.query.IndexQuery;
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
    private int queryLimit;

    private final DeliveryChunkDefinition deliveryChunkDefinition;
    private IndexQuery query;

    public SearchIndexTask(DeliveryChunkDefinition deliveryChunkDefinition, IndexQuery query, IndexStrategyManager indexStrategyManager, int queryLimit) {
        this.deliveryChunkDefinition = deliveryChunkDefinition;
        this.query = query;
        this.indexStrategyManager = indexStrategyManager;
        this.queryLimit = queryLimit;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        long start = System.currentTimeMillis();
        // lookup strategy
        String collection = deliveryChunkDefinition.getCollection();
        String chunkKey = deliveryChunkDefinition.getChunkKey();
        IndexStrategy strategy = indexStrategyManager.getStrategy(collection, chunkKey, query.getName());
        if(strategy == null) {
            throw new JumboIndexMissingException("The queried index '" + query.getName() + "' on collection '" + collection + "' with chunk key '" + chunkKey + "' does not exist.");
        }
        Set<FileOffset> fileOffsets = strategy.findFileOffsets(collection, chunkKey, query, queryLimit);
        log.debug("Searched a full index with " + query.getClauses().size() + " offsets in " + (System.currentTimeMillis() - start) + "ms");
        return fileOffsets;
    }



}