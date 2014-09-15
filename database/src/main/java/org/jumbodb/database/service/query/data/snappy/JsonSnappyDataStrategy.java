package org.jumbodb.database.service.query.data.snappy;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.FutureCancelableTask;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.BetweenDataOperationSearch;
import org.jumbodb.database.service.query.data.common.DataOperationSearch;
import org.jumbodb.database.service.query.data.common.GeoBoundaryBoxDataOperationSearch;
import org.jumbodb.database.service.query.data.common.GeoWithinRangeInMeterDataOperationSearch;
import org.jumbodb.database.service.query.data.common.GtDataOperationSearch;
import org.jumbodb.database.service.query.data.common.EqDataOperationSearch;
import org.jumbodb.database.service.query.data.common.LtDataOperationSearch;
import org.jumbodb.database.service.query.data.common.NeDataOperationSearch;
import org.jumbodb.database.service.query.data.common.OrDataOperationSearch;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.cache.Cache;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class JsonSnappyDataStrategy implements DataStrategy, DataOperationSearch {
    public static final String JSON_SNAPPY_V1 = "JSON_SNAPPY_V1";
    private Logger log = LoggerFactory.getLogger(JsonSnappyDataStrategy.class);

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
        operations.put(QueryOperation.LT, new LtDataOperationSearch());
        operations.put(QueryOperation.BETWEEN, new BetweenDataOperationSearch());
        operations.put(QueryOperation.GEO_BOUNDARY_BOX, new GeoBoundaryBoxDataOperationSearch());
        operations.put(QueryOperation.GEO_WITHIN_RANGE_METER, new GeoWithinRangeInMeterDataOperationSearch());
        return operations;
    }

    @Override
    public boolean isResponsibleFor(String chunkKey, String collection) {
        DeliveryChunkDefinition chunk = collectionDefinition.getChunk(collection, chunkKey);
        if(chunk == null) {
            return false;
        }
        return JSON_SNAPPY_V1.equals(chunk.getDataStrategy());
    }

    @Override
    public String getStrategyName() {
        return JSON_SNAPPY_V1;
    }

    @Override
    public int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        HashMultimap<Integer, FileOffset> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        if (searchQuery.getIndexQuery().size() == 0) {
            log.debug("Running scanned search");
            for (File file : deliveryChunkDefinition.getDataFiles().values()) {
                Future<Integer> future = retrieveDataExecutor.submit(new JsonSnappyRetrieveDataSetsTask(file, Collections.<FileOffset>emptySet(), searchQuery, resultCallback, this, datasetsByOffsetsCache, dataSnappyChunksCache));
                tasks.add(future);
                resultCallback.collect(new FutureCancelableTask(future));
            }
        } else {
            log.debug("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = deliveryChunkDefinition.getDataFiles().get(fileNameHash);
                if (file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<FileOffset> offsets = fileOffsetsMap.get(fileNameHash);
                if (offsets.size() > 0) {
                    Future<Integer> future = retrieveDataExecutor.submit(new JsonSnappyRetrieveDataSetsTask(file, offsets, searchQuery, resultCallback, this, datasetsByOffsetsCache, dataSnappyChunksCache));
                    tasks.add(future);
                    resultCallback.collect(new FutureCancelableTask(future));
                }
            }
        }

        try {
            for (Future<Integer> task : tasks) {
                Integer results = task.get();
                numberOfResults += results;
            }
            log.debug("Collecting " + numberOfResults + " datasets in " + (System.currentTimeMillis() - startTime) + "ms with " + tasks.size() + " threads");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if(cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
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
    public boolean matches(JsonQuery queryClause, Object value) {
        DataOperationSearch jsonOperationSearch = getOperations().get(queryClause.getQueryOperation());
        if(jsonOperationSearch == null) {
            throw new UnsupportedOperationException("OperationSearch is not supported: " + queryClause.getQueryOperation());
        }
        return jsonOperationSearch.matches(queryClause, value);
    }

    @Override
    public long getCompressedSize(File dataFolder) {
        return FileUtils.sizeOfDirectory(dataFolder);
    }

    @Override
    public long getUncompressedSize(File dataFolder) {
        long uncompressedSize = 0l;
        FileFilter metaFiler = FileFilterUtils.makeFileOnly(FileFilterUtils.suffixFileFilter(".chunks"));
        File[] snappyChunks = dataFolder.listFiles(metaFiler);
        for (File snappyChunk : snappyChunks) {
            uncompressedSize += getSizeFromSnappyChunk(snappyChunk);
        }
        return uncompressedSize;
    }

    private long getSizeFromSnappyChunk(File snappyChunk) {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(snappyChunk);
            dis = new DataInputStream(fis);
            return dis.readLong();
        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
        finally {
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
