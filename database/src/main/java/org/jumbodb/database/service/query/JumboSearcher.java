package org.jumbodb.database.service.query;

import com.google.common.collect.HashMultimap;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.query.JumboQuery;
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
    private CollectionDefinition collectionDefinition;
    private ExecutorService retrieveDataSetsExecutor;
    private ExecutorService indexExecutor;
    private ExecutorService chunkExecutor;
    private ObjectMapper jsonMapper;

    public JumboSearcher(File dataPath, File indexPath, IndexStrategyManager indexStrategyManager) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.indexStrategyManager = indexStrategyManager;
        // CARSTEN onInitialize executors in spring
        this.retrieveDataSetsExecutor = Executors.newFixedThreadPool(20);
        this.chunkExecutor = Executors.newCachedThreadPool();
        this.indexExecutor = Executors.newCachedThreadPool();
        // CARSTEN fix, load without index ranges
        this.collectionDefinition = CollectionDefinitionLoader.loadCollectionDefinition(dataPath, indexPath);
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        log.info("IndexedFileSearcher initialized for " + indexPath.getAbsolutePath());
        indexStrategyManager.onInitialize(collectionDefinition);

    }

    public void restart() {
        log.info("IndexedFileSearcher restarting for " + indexPath.getAbsolutePath());
        // CARSTEN fix, load without index ranges
        this.collectionDefinition = CollectionDefinitionLoader.loadCollectionDefinition(dataPath, indexPath);
        indexStrategyManager.onDataChanged(collectionDefinition);
        log.info("IndexedFileSearcher restarted for " + indexPath.getAbsolutePath());
    }

    public void stop() {
        retrieveDataSetsExecutor.shutdown();
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

    private int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        HashMultimap<Integer, Long> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        if(searchQuery.getIndexQuery().size() == 0) {
            log.info("Running scanned search");
            for (File file : deliveryChunkDefinition.getDataFiles().values()) {
                tasks.add(retrieveDataSetsExecutor.submit(new RetrieveDataSetsTask(file, Collections.<Long>emptySet(), searchQuery, resultCallback)));
            }
        }
        else {
            log.info("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = deliveryChunkDefinition.getDataFiles().get(fileNameHash);
                if(file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<Long> offsets = fileOffsetsMap.get(fileNameHash);
                if(offsets.size() > 0) {
                    tasks.add(retrieveDataSetsExecutor.submit(new RetrieveDataSetsTask(file, offsets, searchQuery, resultCallback)));
                }
            }
        }

        try {
            for (Future<Integer> task : tasks) {
                Integer results = task.get();
                numberOfResults += results;
            }
            log.debug("findDataSetsByFileOffsets Time: " + (System.currentTimeMillis() - startTime) + "ms Threads: " + tasks.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return numberOfResults;
    }

    private HashMultimap<Integer, Long> buildFileOffsetsMap(Collection<FileOffset> fileOffsets) {
        HashMultimap<Integer, Long> result = HashMultimap.create();
        for (FileOffset fileOffset : fileOffsets) {
            result.put(fileOffset.getFileNameHash(), fileOffset.getOffset());
        }
        return result;
    }

    private Collection<FileOffset> findFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, JumboQuery searchQuery) {
        if(searchQuery.getIndexQuery().size() == 0) {
            return Collections.emptyList();
        }
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
        for (JumboQuery.IndexQuery indexQuery : searchQuery.getIndexQuery()) {
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
            return findDataSetsByFileOffsets(deliveryChunk, fileOffsets, resultCallback, searchQuery);
        }
    }
}
