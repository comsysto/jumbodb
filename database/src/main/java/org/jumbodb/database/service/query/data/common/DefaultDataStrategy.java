package org.jumbodb.database.service.query.data.common;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.data.common.*;
import org.jumbodb.database.service.query.data.snappy.JsonSnappyLineBreakDataStrategy;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public abstract class DefaultDataStrategy implements DataStrategy {
    private final Map<QueryOperation, DataOperationSearch> OPERATIONS = createOperations();
    protected CollectionDefinition collectionDefinition;
    private Logger log = LoggerFactory.getLogger(JsonSnappyLineBreakDataStrategy.class);

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
    public int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition,
      Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        Map<Integer, Set<FileOffset>> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        boolean dataQueriesAvailable = !searchQuery.getDataQuery().isEmpty();
        boolean indexQueriesAvailable = !searchQuery.getIndexQuery().isEmpty();
        if(dataQueriesAvailable && indexQueriesAvailable) {
            throw new IllegalArgumentException("Top level data queries with combined OR top level index queries are not allowed! It's to inefficient, it results in a full scan, so just use data queries!");
        }
        else if (!dataQueriesAvailable && indexQueriesAvailable) {
            log.debug("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = deliveryChunkDefinition.getDataFiles().get(fileNameHash);
                if (file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<FileOffset> offsets = fileOffsetsMap.get(fileNameHash);
                if (offsets.size() > 0) {
                    submitFileSearchTask(resultCallback, searchQuery, tasks, file, offsets, deliveryChunkDefinition, false);
                }
            }
        } else {
            log.debug("Running scanned search");
            for (File file : deliveryChunkDefinition.getDataFiles().values()) {
                Set<FileOffset> offsets = Collections.emptySet();
                submitFileSearchTask(resultCallback, searchQuery, tasks, file, offsets, deliveryChunkDefinition, true);
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

    protected abstract void submitFileSearchTask(ResultCallback resultCallback, JumboQuery searchQuery, List<Future<Integer>> tasks, File file, Set<FileOffset> offsets, DeliveryChunkDefinition deliveryChunkDefinition, boolean fullScan);

    protected Map<Integer, Set<FileOffset>> buildFileOffsetsMap(Collection<FileOffset> fileOffsets) {
        Map<Integer, Set<FileOffset>> result = new HashMap<Integer, Set<FileOffset>>();
        for (FileOffset fileOffset : fileOffsets) {
            int fileNameHash = fileOffset.getFileNameHash();
            Set<FileOffset> offsets = result.get(fileNameHash);
            if(offsets == null) {
                offsets = new HashSet<FileOffset>();
                result.put(fileNameHash, offsets);
            }
            offsets.add(fileOffset);
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
}
