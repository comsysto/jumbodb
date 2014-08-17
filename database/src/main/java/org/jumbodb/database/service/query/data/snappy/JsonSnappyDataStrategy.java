package org.jumbodb.database.service.query.data.snappy;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.FutureCancelableTask;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
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
public class JsonSnappyDataStrategy implements DataStrategy, JsonOperationSearch {
    public static final String JSON_SNAPPY_V1 = "JSON_SNAPPY_V1";
    private Logger log = LoggerFactory.getLogger(JsonSnappyDataStrategy.class);

    private ExecutorService retrieveDataExecutor;
    private Cache dataSnappyChunksCache;
    private Cache datasetsByOffsetsCache;

    private final Map<QueryOperation, JsonOperationSearch> OPERATIONS = createOperations();
    private CollectionDefinition collectionDefinition;

    private Map<QueryOperation, JsonOperationSearch> createOperations() {
        Map<QueryOperation, JsonOperationSearch> operations = new HashMap<QueryOperation, JsonOperationSearch>();
        operations.put(QueryOperation.EQ, new EqJsonOperationSearch());
        operations.put(QueryOperation.NE, new NeJsonOperationSearch());
        operations.put(QueryOperation.GT, new GtJsonOperationSearch());
        operations.put(QueryOperation.LT, new LtJsonOperationSearch());
        operations.put(QueryOperation.BETWEEN, new BetweenJsonOperationSearch());
        operations.put(QueryOperation.GEO_BOUNDARY_BOX, new GeoBoundaryBoxJsonOperationSearch());
        operations.put(QueryOperation.GEO_WITHIN_RANGE_METER, new GeoWithinRangeInMeterJsonOperationSearch());
        return operations;
    }

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey) {
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

    public Map<QueryOperation, JsonOperationSearch> getOperations() {
        return OPERATIONS;
    }

    @Override
    public boolean matches(QueryClause queryClause, Object value) {
        JsonOperationSearch jsonOperationSearch = getOperations().get(queryClause.getQueryOperation());
        if(jsonOperationSearch == null) {
            throw new UnsupportedOperationException("OperationSearch is not supported: " + queryClause.getQueryOperation());
        }
        return jsonOperationSearch.matches(queryClause, value);
    }

    @Override
    public long getCompressedSize(File dataFolder) {
        // CARSTEN unit test
        return FileUtils.sizeOfDirectory(dataFolder);
    }

    @Override
    public long getUncompressedSize(File dataFolder) {
        // CARSTEN unit test
        long uncompressedSize = 0l;
        FileFilter metaFiler = FileFilterUtils.makeFileOnly(FileFilterUtils.suffixFileFilter(".snappy.chunks"));
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
