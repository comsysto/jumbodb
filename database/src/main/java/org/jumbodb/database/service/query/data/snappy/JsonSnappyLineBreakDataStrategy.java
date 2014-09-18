package org.jumbodb.database.service.query.data.snappy;

import com.google.common.collect.HashMultimap;
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
public class JsonSnappyLineBreakDataStrategy implements DataStrategy {
    public static final String JSON_SNAPPY_LB = "JSON_SNAPPY_LB";
    private Logger log = LoggerFactory.getLogger(JsonSnappyLineBreakDataStrategy.class);

    private ExecutorService retrieveDataExecutor;
    private Cache dataSnappyChunksCache;
    private Cache datasetsByOffsetsCache;

    private final Map<QueryOperation, DataOperationSearch> OPERATIONS = createOperations();
    private CollectionDefinition collectionDefinition;

    private Map<QueryOperation, DataOperationSearch> createOperations() {
        Map<QueryOperation, DataOperationSearch> operations = new HashMap<QueryOperation, DataOperationSearch>();
        operations.put(QueryOperation.OR, new OrDataOperationSearch());
        operations.put(QueryOperation.EQ, new EqDataOperationSearch());
        operations.put(QueryOperation.NE, new NeDataOperationSearch());
        operations.put(QueryOperation.GT, new GtDataOperationSearch());
        operations.put(QueryOperation.GT_EQ, new GtEqDataOperationSearch());
        operations.put(QueryOperation.LT, new LtDataOperationSearch());
        operations.put(QueryOperation.LT_EQ, new LtEqDataOperationSearch());
        operations.put(QueryOperation.BETWEEN, new BetweenDataOperationSearch());
        operations.put(QueryOperation.GEO_BOUNDARY_BOX, new GeoBoundaryBoxDataOperationSearch());
        operations.put(QueryOperation.GEO_WITHIN_RANGE_METER, new GeoWithinRangeInMeterDataOperationSearch());
        return operations;
    }

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
    public int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition,
      Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        HashMultimap<Integer, FileOffset> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        // CARSTEN hier auch noch mal checken ob worklich index search ist ... alte logik
        if (searchQuery.getJsonQuery().isEmpty() && !searchQuery.getIndexQuery().isEmpty()) {
            log.debug("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = deliveryChunkDefinition.getDataFiles().get(fileNameHash);
                if (file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<FileOffset> offsets = fileOffsetsMap.get(fileNameHash);
                if (offsets.size() > 0) {
                    Future<Integer> future = retrieveDataExecutor.submit(
                            new JsonSnappyLineBreakRetrieveDataSetsTask(file, offsets, searchQuery, resultCallback, this,
                                    datasetsByOffsetsCache, dataSnappyChunksCache));
                    tasks.add(future);
                    resultCallback.collect(new FutureCancelableTask(future));
                }
            }
        } else {
            log.debug("Running scanned search");
            for (File file : deliveryChunkDefinition.getDataFiles().values()) {
                Future<Integer> future = retrieveDataExecutor.submit(
                        new JsonSnappyLineBreakRetrieveDataSetsTask(file, Collections.<FileOffset>emptySet(), searchQuery,
                                resultCallback, this, datasetsByOffsetsCache, dataSnappyChunksCache));
                tasks.add(future);
                resultCallback.collect(new FutureCancelableTask(future));
            }
        }

        try {
            for (Future<Integer> task : tasks) {
                Integer results = task.get();
                numberOfResults += results;
            }
            log.debug("Collecting " + numberOfResults + " datasets in " + (System
              .currentTimeMillis() - startTime) + "ms with " + tasks.size() + " threads");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new UnhandledException(cause);
        }
        return numberOfResults;
    }

    private HashMultimap<Integer, FileOffset> buildFileOffsetsMap(Collection<FileOffset> fileOffsets) {
        HashMultimap<Integer, FileOffset> result = HashMultimap.create();
        for (FileOffset fileOffset : fileOffsets) {
            result.put(fileOffset.getFileNameHash(), fileOffset);
        }
        return result;
    }

    public Map<QueryOperation, DataOperationSearch> getOperations() {
        return OPERATIONS;
    }

    @Override
    public boolean matches(QueryOperation operation, Object leftValue, Object rightValue) {
        DataOperationSearch jsonOperationSearch = getOperations().get(operation);
        if (jsonOperationSearch == null) {
            throw new UnsupportedOperationException("OperationSearch is not supported: " + operation.getOperation());
        }
        return jsonOperationSearch.matches(leftValue, rightValue);
    }


    @Override
    public CollectionDataSize getCollectionDataSize(File dataFolder) {
        long compressedSize = FileUtils.sizeOfDirectory(dataFolder);
        long uncompressedSize = 0l;
        long datasets = 0l;
        FileFilter metaFiler = FileFilterUtils.makeFileOnly(FileFilterUtils.suffixFileFilter(".chunks"));
        File[] snappyChunks = dataFolder.listFiles(metaFiler);
        for (File snappyChunk : snappyChunks) {
            SnappyChunkSize sizeFromSnappyChunk = getSizeFromSnappyChunk(snappyChunk);
            uncompressedSize += sizeFromSnappyChunk.uncompressed;
            datasets += sizeFromSnappyChunk.datasets;
        }
        return new CollectionDataSize(datasets, compressedSize, uncompressedSize);
    }

    private static class SnappyChunkSize {
        long uncompressed;
        long datasets;
    }

    private SnappyChunkSize getSizeFromSnappyChunk(File snappyChunk) {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(snappyChunk);
            dis = new DataInputStream(fis);
            SnappyChunkSize snappyChunkSize = new SnappyChunkSize();
            snappyChunkSize.uncompressed = dis.readLong();
            snappyChunkSize.datasets = dis.readLong();
            return snappyChunkSize;
        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(fis);
        }
    }

    @Override
    public List<QueryOperation> getSupportedOperations() {
        return new ArrayList<QueryOperation>(getOperations().keySet());
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
        this.collectionDefinition = collectionDefinition;
    }

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {
        this.collectionDefinition = collectionDefinition;
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
