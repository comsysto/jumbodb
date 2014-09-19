package org.jumbodb.database.service.query.data.snappy;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.FutureCancelableTask;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.CollectionDataSize;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.*;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.cache.Cache;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class JsonSnappyLineBreakDataStrategy extends AbstractJsonSnappyDataStrategy {
    public static final String JSON_SNAPPY_LB = "JSON_SNAPPY_LB";

    private ExecutorService retrieveDataExecutor;
    private Cache dataSnappyChunksCache;
    private Cache datasetsByOffsetsCache;

    @Override
    public boolean isResponsibleFor(String chunkKey, String collection) {
        DeliveryChunkDefinition chunk = collectionDefinition.getChunk(collection, chunkKey);
        if (chunk == null) {
            return false;
        }
        return JSON_SNAPPY_LB.equals(chunk.getDataStrategy());
    }

    @Override
    public String getStrategyName() {
        return JSON_SNAPPY_LB;
    }

    @Override
    protected void submitFileSearchTask(ResultCallback resultCallback, JumboQuery searchQuery,
                                      List<Future<Integer>> tasks, File file, Set<FileOffset> offsets, DeliveryChunkDefinition deliveryChunkDefinition, boolean scannedSearch) {
        Future<Integer> future = retrieveDataExecutor.submit(
                new JsonSnappyLineBreakRetrieveDataSetsTask(file, offsets, searchQuery,
                        resultCallback, this, datasetsByOffsetsCache, dataSnappyChunksCache, deliveryChunkDefinition.getDateFormat(), scannedSearch));
        tasks.add(future);
        resultCallback.collect(new FutureCancelableTask(future));
    }

    @Required
    public void setRetrieveDataExecutor(ExecutorService retrieveDataExecutor) {
        this.retrieveDataExecutor = retrieveDataExecutor;
    }

    @Required
    public void setDataSnappyChunksCache(Cache dataSnappyChunksCache) {
        this.dataSnappyChunksCache = dataSnappyChunksCache;
    }

    @Required
    public void setDatasetsByOffsetsCache(Cache datasetsByOffsetsCache) {
        this.datasetsByOffsetsCache = datasetsByOffsetsCache;
    }


}
