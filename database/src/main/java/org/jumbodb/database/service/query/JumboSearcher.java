package org.jumbodb.database.service.query;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.index.basic.numeric.PseudoCacheForSnappy;
import org.jumbodb.database.service.configuration.JumboConfiguration;
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

import java.util.*;
import java.util.concurrent.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 10:53 AM
 */
public class JumboSearcher {
    private Logger log = LoggerFactory.getLogger(JumboSearcher.class);
    private Logger longRunningLog = LoggerFactory.getLogger("LONG_RUNNING_QUERY");

    private JumboConfiguration jumboConfiguration;
    private IndexStrategyManager indexStrategyManager;
    private DataStrategyManager dataStrategyManager;
    private CollectionDefinition collectionDefinition;
    private ExecutorService indexExecutor;
    private ExecutorService chunkExecutor;
    private ObjectMapper jsonMapper;
    private long queryTimeoutOnChunkInSeconds;


    public void onInitialize() {
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.collectionDefinition = getCollectionDefinition();
        this.indexStrategyManager.onInitialize(collectionDefinition);
        this.dataStrategyManager.onInitialize(collectionDefinition);
    }

    protected CollectionDefinition getCollectionDefinition() {
        return CollectionDefinitionLoader.loadCollectionDefinition(jumboConfiguration.getDataPath(), jumboConfiguration.getIndexPath());
    }

    public void onDataChanged() {
        this.collectionDefinition = getCollectionDefinition();
        this.indexStrategyManager.onDataChanged(collectionDefinition);
        this.dataStrategyManager.onDataChanged(collectionDefinition);
        PseudoCacheForSnappy.clearCache();
    }

    public int findResultAndWriteIntoCallback(String collectionName, JumboQuery searchQuery, ResultCallback resultCallback) {
        Collection<DeliveryChunkDefinition> deliveryChunks = collectionDefinition.getChunks(collectionName);
        if(deliveryChunks != null && deliveryChunks.size() > 0) {
            Collection<SubmittedChunkJob> futures = new LinkedList<SubmittedChunkJob>();
            for (DeliveryChunkDefinition deliveryChunk : deliveryChunks) {
                Future<Integer> future = chunkExecutor.submit(new SearchDeliveryChunkTask(deliveryChunk, searchQuery, resultCallback));
                futures.add(new SubmittedChunkJob(deliveryChunk, future));
            }
            return getNumberOfResultsFromFuturesAndHandleTimeOut(futures, collectionName, searchQuery);
        }
        else {
            throw new JumboCollectionMissingException("Collection '" + collectionName + "' does not exist!");
        }
    }

    private int getNumberOfResultsFromFuturesAndHandleTimeOut(Collection<SubmittedChunkJob> chunkJobs, String collectionName, JumboQuery searchQuery) {
        int results = 0;
        try {
            for (SubmittedChunkJob job : chunkJobs) {
                try {
                    Future<Integer> future = job.getFuture();
                    results += future.get(queryTimeoutOnChunkInSeconds, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    cancelOtherRunningJobs(chunkJobs);
                    StringBuilder buf = new StringBuilder();
                    buf.append("\n############################################################################\n");
                    buf.append("Timed out after: " + queryTimeoutOnChunkInSeconds + " seconds.");
                    buf.append("\n========================== Collection / Chunk ==============================\n");
                    buf.append(job.getDeliveryChunkDefinition());
                    buf.append("\n========================== Jumbo DB Query ==================================\n");
                    buf.append(searchQuery);
                    longRunningLog.warn(buf.toString());
                    throw new JumboCommonException("Query on collection '" + collectionName + "' timed out, see long running query log fr further details.");
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);

        } catch (ExecutionException e) {
            throw (RuntimeException)e.getCause();
        }
        return results;
    }

    private void cancelOtherRunningJobs(Collection<SubmittedChunkJob> chunkJobs) {
        for (SubmittedChunkJob chunkJob : chunkJobs) {
            Future<Integer> future = chunkJob.getFuture();
            future.cancel(true);
        }
    }

    private Collection<FileOffset> findFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, JumboQuery searchQuery) {
        if(searchQuery.getIndexQuery().size() == 0) {
            return Collections.emptyList();
        }
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
        for (IndexQuery indexQuery : searchQuery.getIndexQuery()) {
            tasks.add(indexExecutor.submit(new SearchIndexTask(deliveryChunkDefinition, indexQuery, indexStrategyManager, searchQuery.getLimit())));
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
            throw (RuntimeException)e.getCause();
        }
    }

    protected class SearchDeliveryChunkTask implements Callable<Integer> {
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

    public IndexStrategy getIndexStrategy(String collection, String chunkKey, String indexName) {
        return indexStrategyManager.getStrategy(collection, chunkKey, indexName);
    }

    public IndexStrategy getIndexStrategy(String key) {
        return indexStrategyManager.getStrategy(key);
    }

    public DataStrategy getDataStrategy(String collection, String chunkKey) {
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
    public void setQueryTimeoutOnChunkInSeconds(long queryTimeoutOnChunkInSeconds) {
        this.queryTimeoutOnChunkInSeconds = queryTimeoutOnChunkInSeconds;
    }
}
