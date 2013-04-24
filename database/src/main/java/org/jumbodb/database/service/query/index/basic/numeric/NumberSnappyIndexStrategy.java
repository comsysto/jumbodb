package org.jumbodb.database.service.query.index.basic.numeric;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.jumbodb.database.service.query.index.IndexKey;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.database.service.query.index.integer.snappy.*;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil;
import org.jumbodb.database.service.query.snappy.SnappyStreamToFileCopy;
import org.jumbodb.database.service.query.snappy.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public abstract class NumberSnappyIndexStrategy<T extends Number, IF extends NumberSnappyIndexFile<T>> implements IndexStrategy {

    private Logger log = LoggerFactory.getLogger(NumberSnappyIndexStrategy.class);

    private ExecutorService indexFileExecutor;
    private CollectionDefinition collectionDefinition;
    private Map<IndexKey, List<IF>> indexFiles;

    private final Map<QueryOperation, OperationSearch<T, IF>> OPERATIONS = createOperations();

    private Map<QueryOperation, OperationSearch<T, IF>> createOperations() {
        Map<QueryOperation, OperationSearch<T, IF>> operations = getQueryOperationsStrategies();
        return operations;
    }

    public Set<FileOffset> searchOffsetsByClauses(File indexFile, Set<QueryClause> clauses) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        Set<FileOffset> result = new HashSet<FileOffset>();
        try {
            SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(indexFile);
            raf = new RandomAccessFile(indexFile, "r");

            for (QueryClause clause : clauses) {
                result.addAll(findOffsetForClause(raf, clause, snappyChunks));
            }

        } finally {
            IOUtils.closeQuietly(raf);
        }
        log.info("Time for search one index part-file offsets " + result.size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {
        IndexDefinition chunkIndex = collectionDefinition.getChunkIndex(collection, chunkKey, indexName);
        if(chunkIndex != null) {
            return getStrategyName().equals(chunkIndex.getStrategy());
        }
        return false;
    }

    private Map<IndexKey, List<IF>> buildIndexRanges() {
        Map<IndexKey, List<IF>> result = Maps.newHashMap();
        for (String collection : collectionDefinition.getCollections()) {
            for (DeliveryChunkDefinition deliveryChunkDefinition : collectionDefinition.getChunks(collection)) {
                for (IndexDefinition indexDefinition : deliveryChunkDefinition.getIndexes()) {
                    if(getStrategyName().equals(indexDefinition.getStrategy())) {
                        IndexKey indexKey = new IndexKey(collection, deliveryChunkDefinition.getChunkKey(), indexDefinition.getName());
                        result.put(indexKey, buildIndexRange(indexDefinition.getPath()));
                    }
                }
            }
        }


        return result;
    }

    private List<IF> buildIndexRange(File indexFolder) {
        List<IF> result = new LinkedList<IF>();
        File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".odx"));
        for (File indexFile : indexFiles) {
                SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(indexFile);
                if(snappyChunks.getNumberOfChunks() > 0) {
                    result.add(createIndexFileDescription(indexFile, snappyChunks));
                }
        }
        return result;
    }

    private IF createIndexFileDescription(File indexFile, SnappyChunks snappyChunks) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(indexFile, "r");
            byte[] uncompressed = SnappyUtil.getUncompressed(raf, snappyChunks, 0);
            T from = readFirstValue(uncompressed);
            uncompressed = SnappyUtil.getUncompressed(raf, snappyChunks, snappyChunks.getNumberOfChunks() - 1);
            T to = readLastValue(uncompressed);
            return createIndexFile(from, to, indexFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(raf);
        }
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query) {
        try {
            MultiValueMap<File, QueryClause> groupedByIndexFile = groupByIndexFile(collection, chunkKey, query);
            List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
            for (File indexFile : groupedByIndexFile.keySet()) {
                tasks.add(indexFileExecutor.submit(new NumberSnappyIndexTask(this, indexFile, new HashSet<QueryClause>(groupedByIndexFile.get(indexFile)))));
            }
            Set<FileOffset> result = new HashSet<FileOffset>();
            for (Future<Set<FileOffset>> task : tasks) {
                result.addAll(task.get());
            }
            return result;
        } catch(Exception ex) {
            throw new UnhandledException(ex);
        }
    }

    public MultiValueMap<File, QueryClause> groupByIndexFile(String collection, String chunkKey, IndexQuery query) {
        List<IF> indexFiles = getIndexFiles(collection, chunkKey, query);
        MultiValueMap<File, QueryClause> groupByIndexFile = new LinkedMultiValueMap<File, QueryClause>();
        for (IF hashCodeSnappyIndexFile : indexFiles) {
            for (QueryClause queryClause : query.getClauses()) {
                if(acceptIndexFile(queryClause, hashCodeSnappyIndexFile)) {
                    groupByIndexFile.add(hashCodeSnappyIndexFile.getIndexFile(), queryClause);
                }
            }
        }
        return groupByIndexFile;
    }

    private List<IF> getIndexFiles(String collection, String chunkKey, IndexQuery query) {
        return indexFiles.get(new IndexKey(collection, chunkKey, query.getName()));
    }


    private Set<FileOffset> findOffsetForClause(RandomAccessFile indexRaf, QueryClause clause, SnappyChunks snappyChunks) throws IOException {
        OperationSearch<T, IF> integerOperationSearch = OPERATIONS.get(clause.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + clause.getQueryOperation());
        }
        long currentChunk = integerOperationSearch.findFirstMatchingChunk(indexRaf, clause, snappyChunks);
        long numberOfChunks = snappyChunks.getNumberOfChunks();
        if(currentChunk >= 0) {
            Set<FileOffset> result = new HashSet<FileOffset>();
            while(currentChunk < numberOfChunks) {
                byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
                ByteArrayInputStream bais = null;
                DataInputStream dis = null;
                try {
                    bais = new ByteArrayInputStream(uncompressed);
                    dis = new DataInputStream(bais);
                    while(bais.available() > 0) {
                        T currentIntValue = readValueFromDataInputStream(dis);
                        int fileNameHash = dis.readInt();
                        long offset = dis.readLong();
                        if(integerOperationSearch.matching(currentIntValue, clause)) {
                            result.add(new FileOffset(fileNameHash, offset));
                        } else if(!result.isEmpty()) {
                            // found some results, but here it isnt equal, that means end of results
                            return result;
                        }
                    }
                } finally {
                    IOUtils.closeQuietly(dis);
                    IOUtils.closeQuietly(bais);
                }

                currentChunk++;
            }
            return result;
        }
        return Collections.emptySet();
    }

    public boolean acceptIndexFile(QueryClause queryClause, IF hashCodeSnappyIndexFile) {
        OperationSearch<T, IF> integerOperationSearch = OPERATIONS.get(queryClause.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + queryClause.getQueryOperation());
        }
        return integerOperationSearch.acceptIndexFile(queryClause, hashCodeSnappyIndexFile);
    }

    @Override
    public Set<QueryOperation> getSupportedOperations() {
        return OPERATIONS.keySet();
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
        onDataChanged(collectionDefinition);
    }

    @Override
    public void onImport(ImportMetaFileInformation information, InputStream dataInputStream, File absoluteImportPathFile) {
        String absoluteImportPath = absoluteImportPathFile.getAbsolutePath() + "/" + information.getFileName();
        SnappyStreamToFileCopy.copy(dataInputStream, new File(absoluteImportPath), information.getFileLength(), getSnappyChunkSize());
    }

    public abstract Map<QueryOperation, OperationSearch<T, IF>> getQueryOperationsStrategies();
    public abstract int getSnappyChunkSize();
    public abstract T readValueFromDataInputStream(DataInputStream dis) throws IOException;
    public abstract T readLastValue(byte[] uncompressed);
    public abstract T readFirstValue(byte[] uncompressed);
    public abstract IF createIndexFile(T from, T to, File indexFile);

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {
        this.collectionDefinition = collectionDefinition;
        this.indexFiles = buildIndexRanges();
    }


    @Required
    public void setIndexFileExecutor(ExecutorService indexFileExecutor) {
        this.indexFileExecutor = indexFileExecutor;
    }
}
