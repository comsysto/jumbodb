package org.jumbodb.database.service.query.data.lz4;

import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.FutureCancelableTask;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.snappy.AbstractJsonSnappyDataStrategy;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.cache.Cache;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class JsonLz4LineBreakDataStrategy extends AbstractJsonSnappyDataStrategy {
    public static final String JSON_LZ4_LB = "JSON_LZ4_LB";

    private ExecutorService retrieveDataExecutor;
    private Cache dataCompressionBlocksCache;
    private Cache datasetsByOffsetsCache;

    @Override
    public String getStrategyName() {
        return JSON_LZ4_LB;
    }

    @Override
    protected void submitFileSearchTask(ResultCallback resultCallback, JumboQuery searchQuery,
                                        List<Future<Integer>> tasks, File file, Set<FileOffset> offsets, DeliveryChunkDefinition deliveryChunkDefinition, boolean scannedSearch) {
        Future<Integer> future = retrieveDataExecutor.submit(
                new JsonLz4LineBreakRetrieveDataSetsTask(file, offsets, searchQuery,
                        resultCallback, this, datasetsByOffsetsCache, dataCompressionBlocksCache, deliveryChunkDefinition.getDateFormat(), scannedSearch));
        tasks.add(future);
        resultCallback.collect(new FutureCancelableTask(future));
    }

    @Required
    public void setRetrieveDataExecutor(ExecutorService retrieveDataExecutor) {
        this.retrieveDataExecutor = retrieveDataExecutor;
    }

    @Required
    public void setDataCompressionBlocksCache(Cache dataCompressionBlocksCache) {
        this.dataCompressionBlocksCache = dataCompressionBlocksCache;
    }

    @Required
    public void setDatasetsByOffsetsCache(Cache datasetsByOffsetsCache) {
        this.datasetsByOffsetsCache = datasetsByOffsetsCache;
    }


}