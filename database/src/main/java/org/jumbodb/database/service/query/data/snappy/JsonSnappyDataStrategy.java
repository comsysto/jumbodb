package org.jumbodb.database.service.query.data.snappy;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.database.service.query.snappy.SnappyStreamToFileCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class JsonSnappyDataStrategy implements DataStrategy, JsonOperationSearch {
    public static final int SNAPPY_DATA_CHUNK_SIZE = 32 * 1024;
    public static final String JSON_SNAPPY_V1 = "JSON_SNAPPY_V1";
    private Logger log = LoggerFactory.getLogger(JsonSnappyDataStrategy.class);

    private ExecutorService retrieveDataExecutor;

    private final Map<QueryOperation, JsonOperationSearch> OPERATIONS = createOperations();

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
    public void onImport(ImportMetaFileInformation information, InputStream dataInputStream, File absoluteImportPathFile) {
        String absoluteImportPath = absoluteImportPathFile.getAbsolutePath() + "/" + information.getFileName();
        SnappyStreamToFileCopy.copy(dataInputStream, new File(absoluteImportPath), information.getFileLength(), SNAPPY_DATA_CHUNK_SIZE);
    }

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey) {
        return true;
    }

    @Override
    public String getStrategyName() {
        return JSON_SNAPPY_V1;
    }

    @Override
    public int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        HashMultimap<Integer, Long> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        if (searchQuery.getIndexQuery().size() == 0) {
            log.debug("Running scanned search");
            for (File file : deliveryChunkDefinition.getDataFiles().values()) {
                tasks.add(retrieveDataExecutor.submit(new JsonSnappyRetrieveDataSetsTask(file, Collections.<Long>emptySet(), searchQuery, resultCallback, this)));
            }
        } else {
            log.debug("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = deliveryChunkDefinition.getDataFiles().get(fileNameHash);
                if (file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<Long> offsets = fileOffsetsMap.get(fileNameHash);
                if (offsets.size() > 0) {
                    tasks.add(retrieveDataExecutor.submit(new JsonSnappyRetrieveDataSetsTask(file, offsets, searchQuery, resultCallback, this)));
                }
            }
        }

        try {
            for (Future<Integer> task : tasks) {
                Integer results = task.get();
                numberOfResults += results;
            }
            log.debug("Collecting " + numberOfResults + " datasets in " + (System.currentTimeMillis() - startTime) + "ms with " + tasks.size() + " threads: ");
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

    @Override
    public List<QueryOperation> getSupportedOperations() {
        return new ArrayList<QueryOperation>(OPERATIONS.keySet());
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
    }

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {
    }

    @Required
    public void setRetrieveDataExecutor(ExecutorService retrieveDataExecutor) {
        this.retrieveDataExecutor = retrieveDataExecutor;
    }

    @Override
    public boolean matches(QueryClause queryClause, Object value) {
        JsonOperationSearch jsonOperationSearch = OPERATIONS.get(queryClause.getQueryOperation());
        if(jsonOperationSearch == null) {
            throw new UnsupportedOperationException("OperationSearch is not supported: " + queryClause.getQueryOperation());
        }
        return jsonOperationSearch.matches(queryClause, value);
    }
}
