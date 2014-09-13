package org.jumbodb.database.service.query;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.database.service.query.index.IndexStrategyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class SearchIndexTask implements Callable<Set<FileOffset>> {
    private Logger log = LoggerFactory.getLogger(SearchIndexTask.class);

    private IndexStrategyManager indexStrategyManager;
    private int queryLimit;
    private boolean resultCacheEnabled;
    private DeliveryChunkDefinition deliveryChunkDefinition;
    private String indexName;
    private List<IndexQuery> indexQueries;

    public SearchIndexTask(DeliveryChunkDefinition deliveryChunkDefinition, String indexName, List<IndexQuery> indexQueries,
                           IndexStrategyManager indexStrategyManager, int queryLimit, boolean resultCacheEnabled) {
        this.deliveryChunkDefinition = deliveryChunkDefinition;
        this.indexName = indexName;
        this.indexQueries = indexQueries;
        this.indexStrategyManager = indexStrategyManager;
        this.queryLimit = queryLimit;

        this.resultCacheEnabled = resultCacheEnabled;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        long start = System.currentTimeMillis();
        // lookup strategy
        String collection = deliveryChunkDefinition.getCollection();
        String chunkKey = deliveryChunkDefinition.getChunkKey();
        IndexStrategy strategy = indexStrategyManager.getStrategy(chunkKey, collection, indexName);
        if (strategy == null) {
            throw new JumboIndexMissingException("The queried index '" + indexName + "' on collection '" + collection + "' with chunk key '" + chunkKey + "' does not exist.");
        }
        Set<FileOffset> fileOffsets = strategy.findFileOffsets(chunkKey, collection, indexName, indexQueries, queryLimit, resultCacheEnabled);
        log.debug("Searched a full index with " + fileOffsets.size() + " offsets in " + (System.currentTimeMillis() - start) + "ms");
        return fileOffsets;
    }


}