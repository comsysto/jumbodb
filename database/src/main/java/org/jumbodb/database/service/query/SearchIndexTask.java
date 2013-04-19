package org.jumbodb.database.service.query;

import org.jumbodb.connector.query.JumboQuery;
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
    private JumboQuery.IndexQuery query;

    public SearchIndexTask(DeliveryChunkDefinition deliveryChunkDefinition, JumboQuery.IndexQuery query, IndexStrategyManager indexStrategyManager) {
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
        // find ausführen

//        MultiValueMap<File, Integer> groupedByIndexFile = SearchIndexUtils.groupByIndexFile(deliveryChunkDefinition, query);
//        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
//        for (File indexFile : groupedByIndexFile.keySet()) {
//            tasks.add(indexFileExecutor.submit(new SearchIndexFileTask(indexFile, new HashSet<Integer>(groupedByIndexFile.get(indexFile)))));
//        }
//        Set<FileOffset> result = new HashSet<FileOffset>();
//        for (Future<Set<FileOffset>> task : tasks) {
//            result.addAll(task.get());
//        }
        log.info("Time for search one complete index offsets " + query.getClauses().size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return fileOffsets;
    }



}