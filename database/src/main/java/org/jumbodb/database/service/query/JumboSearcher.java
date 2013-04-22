package org.jumbodb.database.service.query;

import com.google.common.collect.HashMultimap;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.DataStrategyManager;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.CollectionDefinitionLoader;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.index.IndexStrategyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 10:53 AM
 */
public class JumboSearcher {
    private Logger log = LoggerFactory.getLogger(JumboSearcher.class);

    private final File dataPath;
    private final File indexPath;
    private IndexStrategyManager indexStrategyManager;
    private DataStrategyManager dataStrategyManager;
    private CollectionDefinition collectionDefinition;
    private ExecutorService indexExecutor;
    private ExecutorService chunkExecutor;
    private ObjectMapper jsonMapper;

    public JumboSearcher(File dataPath, File indexPath, IndexStrategyManager indexStrategyManager, DataStrategyManager dataStrategyManager) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.indexStrategyManager = indexStrategyManager;
        this.dataStrategyManager = dataStrategyManager;
        // CARSTEN onInitialize executors in spring
        this.chunkExecutor = Executors.newCachedThreadPool();
        this.indexExecutor = Executors.newCachedThreadPool();
        this.collectionDefinition = CollectionDefinitionLoader.loadCollectionDefinition(dataPath, indexPath);
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        log.info("IndexedFileSearcher initialized for " + indexPath.getAbsolutePath());
        indexStrategyManager.onInitialize(collectionDefinition);

    }

    public void stop() {
        chunkExecutor.shutdown();
        indexExecutor.shutdown();
    }

    public int findResultAndWriteIntoCallback(String collectionName, JumboQuery searchQuery, ResultCallback resultCallback) {
        Collection<DeliveryChunkDefinition> deliveryChunks = collectionDefinition.getChunks(collectionName);
        if(deliveryChunks != null) {
            Collection<Future<Integer>> futures = new LinkedList<Future<Integer>>();
            for (DeliveryChunkDefinition deliveryChunk : deliveryChunks) {
                futures.add(chunkExecutor.submit(new SearchDeliveryChunkTask(deliveryChunk, searchQuery, resultCallback)));
            }
            return getNumberOfResultsFromFutures(futures);
        }
        return 0;
    }

    private int getNumberOfResultsFromFutures(Collection<Future<Integer>> futures) {
        int results = 0;
        try {
            for (Future<Integer> future : futures) {
                results += future.get();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return results;
    }

    private Collection<FileOffset> findFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, JumboQuery searchQuery) {
        if(searchQuery.getIndexQuery().size() == 0) {
            return Collections.emptyList();
        }
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
        for (IndexQuery indexQuery : searchQuery.getIndexQuery()) {
            tasks.add(indexExecutor.submit(new SearchIndexTask(deliveryChunkDefinition, indexQuery, indexStrategyManager)));
        }

        try {
            Collection<FileOffset> result = null;
            for (Future<Set<FileOffset>> task : tasks) {
                // .get is blocking, so we dont have to wait explicitly
                Set<FileOffset> fileOffsets = task.get();
                if(result == null) {
                    result = new HashSet<FileOffset>(fileOffsets);
                }
                else {
                    result.retainAll(fileOffsets);
                }
            }
            return result;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private class SearchDeliveryChunkTask implements Callable<Integer> {
        private DeliveryChunkDefinition deliveryChunk;
        private JumboQuery searchQuery;
        private ResultCallback resultCallback;

        private SearchDeliveryChunkTask(DeliveryChunkDefinition deliveryChunk, JumboQuery searchQuery, ResultCallback resultCallback) {
            this.deliveryChunk = deliveryChunk;
            this.searchQuery = searchQuery;
            this.resultCallback = resultCallback;
        }

        @Override
        public Integer call() throws Exception {
            Collection<FileOffset> fileOffsets = findFileOffsets(deliveryChunk, searchQuery);
            DataStrategy strategy = dataStrategyManager.getStrategy(deliveryChunk.getCollection(), deliveryChunk.getChunkKey());
            return strategy.findDataSetsByFileOffsets(deliveryChunk, fileOffsets, resultCallback, searchQuery);
        }
    }
}
