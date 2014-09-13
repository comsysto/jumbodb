package org.jumbodb.database.service.query;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.data.common.meta.CollectionProperties;
import org.jumbodb.data.common.meta.IndexProperties;
import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.DataStrategyManager;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.CollectionDefinitionLoader;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.database.service.query.index.IndexStrategyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.cache.CacheManager;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 10:53 AM
 */
public class JumboSearcher {
    private Logger log = LoggerFactory.getLogger(JumboSearcher.class);

    private JumboConfiguration jumboConfiguration;
    private IndexStrategyManager indexStrategyManager;
    private DataStrategyManager dataStrategyManager;
    private CollectionDefinition collectionDefinition;
    private ExecutorService indexExecutor;
    private ExecutorService chunkExecutor;
    private CacheManager cacheManager;

    public void onInitialize() {
        this.collectionDefinition = getCollectionDefinition();
        this.indexStrategyManager.onInitialize(collectionDefinition);
        this.dataStrategyManager.onInitialize(collectionDefinition);
    }

    protected CollectionDefinition getCollectionDefinition() {
        return CollectionDefinitionLoader
          .loadCollectionDefinition(jumboConfiguration.getDataPath(), jumboConfiguration.getIndexPath());
    }

    public void onDataChanged() {
        this.collectionDefinition = getCollectionDefinition();
        this.indexStrategyManager.onDataChanged(collectionDefinition);
        this.dataStrategyManager.onDataChanged(collectionDefinition);
        Collection<String> cacheNames = cacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            cacheManager.getCache(cacheName).clear();
        }
    }

    public int findResultAndWriteIntoCallback(JumboQuery searchQuery,
      ResultCallback resultCallback) {
        Collection<DeliveryChunkDefinition> deliveryChunks = collectionDefinition.getChunks(searchQuery.getCollection());
        if (deliveryChunks != null && deliveryChunks.size() > 0) {
            List<Future<Integer>> futures = new LinkedList<Future<Integer>>();
            for (DeliveryChunkDefinition deliveryChunk : deliveryChunks) {
                Future<Integer> future = chunkExecutor
                  .submit(new SearchDeliveryChunkTask(deliveryChunk, searchQuery, resultCallback));
                futures.add(future);
                resultCallback.collect(new FutureCancelableTask(future));
            }
            return getNumberOfResultsFromFuturesAndHandleTimeOut(futures);
        } else {
            throw new JumboCollectionMissingException("Collection '" + searchQuery.getCollection() + "' does not exist!");
        }
    }

    private int getNumberOfResultsFromFuturesAndHandleTimeOut(List<Future<Integer>> futures) {
        int results = 0;
        try {
            for (Future<Integer> future : futures) {
                results += future.get();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
        return results;
    }

    private Collection<FileOffset> findFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition,
      JumboQuery searchQuery, ResultCallback resultCallback) {
        if (searchQuery.getIndexQuery().size() == 0) {
            return Collections.emptyList();
        }
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
//        for (IndexQuery indexQuery : searchQuery.getIndexQuery()) {
        Map<String, List<IndexQuery>> indexGroupedByIndexName = groupByIndexName(searchQuery);
        for (String indexName : indexGroupedByIndexName.keySet()) {
            List<IndexQuery> indexQueries = indexGroupedByIndexName.get(indexName);
            Future<Set<FileOffset>> future = indexExecutor.submit(
                    new SearchIndexTask(deliveryChunkDefinition, indexName, indexQueries, indexStrategyManager, searchQuery.getLimit(),
                            searchQuery.isResultCacheEnabled()));
            tasks.add(future);
            resultCallback.collect(new FutureCancelableTask(future));
        }

//        }

        try {
            Collection<FileOffset> result = null;
            for (Future<Set<FileOffset>> task : tasks) {
                // .get is blocking, so we dont have to wait explicitly
                Set<FileOffset> fileOffsets = task.get();
                if (result == null) {
                    result = new HashSet<FileOffset>(fileOffsets);
                } else {
                    result.addAll(fileOffsets);
                }
            }
            return filterOffsets(result, searchQuery);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    // CARSTEN unit test
    // CARSTEN ab hier durchdebuggen und schauen ob das verhalten ok ist
    private Collection<FileOffset> filterOffsets(Collection<FileOffset> fileOffsets, JumboQuery searchQuery) {
        Collection<FileOffset> result = new HashSet<FileOffset>();
        Map<IndexQuery, List<FileOffset>> groupByIndexQuery = groupByIndexQuery(result);

        for (IndexQuery indexQuery : searchQuery.getIndexQuery()) {
            result.addAll(getIndexesApplyAndLogic(indexQuery, fileOffsets, groupByIndexQuery));
        }
        return result;
    }

    private Set<FileOffset> getIndexesApplyAndLogic(IndexQuery indexQuery, Collection<FileOffset> fileOffsets, Map<IndexQuery, List<FileOffset>> groupByIndexQuery) {
        Set<FileOffset> result = new HashSet<FileOffset>();
        IndexQuery andIndex = indexQuery.getAndIndex();
        List<FileOffset> fileOffset = groupByIndexQuery.get(indexQuery);
        result.addAll(fileOffset);
        if(andIndex != null) {
            result.retainAll(getIndexesApplyAndLogic(andIndex, fileOffsets, groupByIndexQuery));
        }
        return result;
    }

    // CARSTEN unit test
    private Map<IndexQuery, List<FileOffset>> groupByIndexQuery(Collection<FileOffset> fileOffsets) {
        Map<IndexQuery, List<FileOffset>> result = new HashMap<IndexQuery, List<FileOffset>>();
        for (FileOffset fileOffset : fileOffsets) {
            List<FileOffset> tmpOffsets = result.get(fileOffset.getIndexQuery());
            if(tmpOffsets == null) {
                tmpOffsets = new LinkedList<FileOffset>();
                result.put(fileOffset.getIndexQuery(), tmpOffsets);
            }
            tmpOffsets.add(fileOffset);
        }
        return result;
    }

    // CARSTEN unit test
    private Map<String, List<IndexQuery>> groupByIndexName(JumboQuery searchQuery) {
        Map<String, List<IndexQuery>> result = new HashMap<String, List<IndexQuery>>();
        List<IndexQuery> allIndexQueries = getAllIndexQueries(searchQuery.getIndexQuery());
        for (IndexQuery indexQuery : allIndexQueries) {
            List<IndexQuery> indexQueries = result.get(indexQuery.getName());
            if(indexQueries == null) {
                indexQueries = new LinkedList<IndexQuery>();
                result.put(indexQuery.getName(), indexQueries);
            }
            indexQueries.add(indexQuery);
        }
        return result;
    }

    private List<IndexQuery> getAllIndexQueries(List<IndexQuery> indexQuery) {
        List<IndexQuery> result = new LinkedList<IndexQuery>();
        for (IndexQuery query : indexQuery) {
            result.addAll(getAllIndexQueries(query));
        }
        return result;
    }

    private List<IndexQuery> getAllIndexQueries(IndexQuery query) {
        List<IndexQuery> result = new LinkedList<IndexQuery>();
        result.add(query);
        IndexQuery andIndex = query.getAndIndex();
        if(andIndex != null) {
            result.addAll(getAllIndexQueries(andIndex));
        }
        return result;
    }

    protected class SearchDeliveryChunkTask implements Callable<Integer> {
        private DeliveryChunkDefinition deliveryChunk;
        private JumboQuery searchQuery;
        private ResultCallback resultCallback;

        private SearchDeliveryChunkTask(DeliveryChunkDefinition deliveryChunk, JumboQuery searchQuery,
          ResultCallback resultCallback) {
            this.deliveryChunk = deliveryChunk;
            this.searchQuery = searchQuery;
            this.resultCallback = resultCallback;
        }

        @Override
        public Integer call() throws Exception {
            Collection<FileOffset> fileOffsets = findFileOffsets(deliveryChunk, searchQuery, resultCallback);
            DataStrategy strategy = dataStrategyManager
              .getStrategy(deliveryChunk.getCollection(), deliveryChunk.getChunkKey());
            return strategy.findDataSetsByFileOffsets(deliveryChunk, fileOffsets, resultCallback, searchQuery);
        }
    }

    public long getDataCompressedSize(String chunkKey, String version, String collection) {
        File file = buildPathToData(chunkKey, version, collection);
        String strategyName = getDataStrategyName(file);
        DataStrategy dataStrategy = getDataStrategy(strategyName);
        return dataStrategy.getCompressedSize(file);
    }

    public long getDataUncompressedSize(String chunkKey, String version, String collection) {
        File file = buildPathToData(chunkKey, version, collection);
        String strategyName = getDataStrategyName(file);
        DataStrategy dataStrategy = getDataStrategy(strategyName);
        return dataStrategy.getUncompressedSize(file);
    }

    public long getIndexSize(String chunkKey, String version, String collection) {
        long result = 0l;
        File indexRoot = buildPathToIndexRoot(chunkKey, version, collection);
        File[] indexFolders = indexRoot.listFiles(StorageManagement.FOLDER_FILTER);
        for (File indexFolder : indexFolders) {
            result += getIndexSize(indexFolder);
        }
        return result;
    }

    private long getIndexSize(File indexFolder) {
        String indexStrategyName = getIndexStrategyName(indexFolder);
        IndexStrategy indexStrategy = getIndexStrategy(indexStrategyName);
        return indexStrategy.getSize(indexFolder);
    }

    private String getIndexStrategyName(File indexPath) {
        File indexProps = new File(indexPath.getAbsolutePath() + "/" + IndexProperties.DEFAULT_FILENAME);
        return IndexProperties.getStrategy(indexProps);
    }

    private String getDataStrategyName(File dataPath) {
        File collectionProps = new File(dataPath.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME);
        return CollectionProperties.getStrategy(collectionProps);
    }

    private File buildPathToData(String chunkKey, String version, String collection) {
        return new File(
          jumboConfiguration.getDataPath().getAbsolutePath() + "/" + chunkKey + "/" + version + "/" + collection + "/");
    }

    private File buildPathToIndexRoot(String chunkKey, String version, String collection) {
        return new File(jumboConfiguration.getIndexPath().getAbsolutePath() + "/" + chunkKey + "/" + version + "/" + collection + "/");
    }

    public IndexStrategy getIndexStrategy(String chunkKey, String collection, String indexName) {
        return indexStrategyManager.getStrategy(chunkKey, collection, indexName);
    }

    public IndexStrategy getIndexStrategy(String key) {
        return indexStrategyManager.getStrategy(key);
    }

    public DataStrategy getDataStrategy(String key) {
        return dataStrategyManager.getStrategy(key);
    }

    public DataStrategy getDataStrategy(String chunkKey, String collection) {
        return dataStrategyManager.getStrategy(collection, chunkKey);
    }

    @Required
    public void setJumboConfiguration(JumboConfiguration jumboConfiguration) {
        this.jumboConfiguration = jumboConfiguration;
    }

    @Required
    public void setIndexStrategyManager(IndexStrategyManager indexStrategyManager) {
        this.indexStrategyManager = indexStrategyManager;
    }

    @Required
    public void setDataStrategyManager(DataStrategyManager dataStrategyManager) {
        this.dataStrategyManager = dataStrategyManager;
    }

    @Required
    public void setIndexExecutor(ExecutorService indexExecutor) {
        this.indexExecutor = indexExecutor;
    }

    @Required
    public void setChunkExecutor(ExecutorService chunkExecutor) {
        this.chunkExecutor = chunkExecutor;
    }

    @Required
    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
}
