package org.jumbodb.database.service.query.index.basic.numeric;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.snappy.SnappyChunksUtil;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.jumbodb.database.service.query.index.IndexKey;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public abstract class NumberSnappyIndexStrategy<T, IFV, IF extends NumberSnappyIndexFile<IFV>> implements IndexStrategy {

    private Logger log = LoggerFactory.getLogger(NumberSnappyIndexStrategy.class);

    private ExecutorService indexFileExecutor;
    private CollectionDefinition collectionDefinition;
    private Map<IndexKey, List<IF>> indexFiles;

    protected final Map<QueryOperation, OperationSearch<T, IFV, IF>> OPERATIONS = createOperations();

    private Map<QueryOperation, OperationSearch<T, IFV, IF>> createOperations() {
        Map<QueryOperation, OperationSearch<T, IFV, IF>> operations = getQueryOperationsStrategies();
        return operations;
    }

    public Set<FileOffset> searchOffsetsByClauses(File indexFile, Set<QueryClause> clauses, int queryLimit) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        List<FileOffset> result = new LinkedList<FileOffset>();
        try {
            SnappyChunks snappyChunks = PseudoCacheForSnappy.getSnappyChunksByFile(indexFile);
            raf = new RandomAccessFile(indexFile, "r");

            for (QueryClause clause : clauses) {
                if(queryLimit == -1 || queryLimit > result.size()) {
                    result.addAll(findOffsetForClause(indexFile, raf, clause, snappyChunks, queryLimit));
                }
            }

        } finally {
            IOUtils.closeQuietly(raf);
        }
        log.trace("Search one index part-file with " + result.size() + " offsets in " + (System.currentTimeMillis() - start) + "ms");
        return new HashSet<FileOffset>(result);
    }

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {
        IndexDefinition chunkIndex = collectionDefinition.getChunkIndex(collection, chunkKey, indexName);
        if(chunkIndex != null) {
            return getStrategyName().equals(chunkIndex.getStrategy());
        }
        return false;
    }

    protected Map<IndexKey, List<IF>> buildIndexRanges() {
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

    protected List<IF> buildIndexRange(File indexFolder) {
        List<IF> result = new LinkedList<IF>();
        File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".odx"));
        for (File indexFile : indexFiles) {
            SnappyChunks snappyChunks = PseudoCacheForSnappy.getSnappyChunksByFile(indexFile);
            if(snappyChunks.getNumberOfChunks() > 0) {
                result.add(createIndexFileDescription(indexFile, snappyChunks));
            }
        }
        return result;
    }

    protected IF createIndexFileDescription(File indexFile, SnappyChunks snappyChunks) {
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
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query, int queryLimit) {
        try {
            MultiValueMap<File, QueryClause> groupedByIndexFile = groupByIndexFile(collection, chunkKey, query);
            List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();

            for (Map.Entry<File, List<QueryClause>> clausesPerIndexFile : groupedByIndexFile.entrySet()) {
                tasks.add(indexFileExecutor.submit(new NumberSnappyIndexTask(this, clausesPerIndexFile.getKey(),
                        new HashSet<QueryClause>(clausesPerIndexFile.getValue()), queryLimit)));
            }
            Set<FileOffset> result = new HashSet<FileOffset>();
            for (Future<Set<FileOffset>> task : tasks) {
                result.addAll(task.get());
            }
            return result;
        } catch(ExecutionException e) {
            throw (RuntimeException)e.getCause();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public MultiValueMap<File, QueryClause> groupByIndexFile(String collection, String chunkKey, IndexQuery query) {
        List<IF> indexFiles = getIndexFiles(collection, chunkKey, query);
        MultiValueMap<File, QueryClause> groupByIndexFile = new LinkedMultiValueMap<File, QueryClause>();
        for (IF indexFile : indexFiles) {
            for (QueryClause queryClause : query.getClauses()) {
                if(acceptIndexFile(queryClause, indexFile)) {
                    groupByIndexFile.add(indexFile.getIndexFile(), queryClause);
                }
            }
        }
        return groupByIndexFile;
    }

    protected List<IF> getIndexFiles(String collection, String chunkKey, IndexQuery query) {
        return indexFiles.get(new IndexKey(collection, chunkKey, query.getName()));
    }


    protected Set<FileOffset> findOffsetForClause(final File indexFile, final RandomAccessFile indexRaf, QueryClause clause, final SnappyChunks snappyChunks, int queryLimit) throws IOException {
        OperationSearch<T, IFV, IF> integerOperationSearch = OPERATIONS.get(clause.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + clause.getQueryOperation());
        }
        QueryValueRetriever queryValueRetriever = integerOperationSearch.getQueryValueRetriever(clause);
        FileDataRetriever fileDataRetriever = new FileDataRetriever() {
//            @Override
            public byte[] getUncompressedBlock(long searchChunk) throws IOException {
                return SnappyUtil.getUncompressed(indexRaf, snappyChunks, searchChunk);
            }

            @Override
            public BlockRange<T> getBlockRange(long searchChunk) throws IOException {
                BlockRange<?> snappyChunkRange = PseudoCacheForSnappy.getSnappyChunkRange(indexFile, searchChunk);
                log.trace("getBlockRange " + indexFile.getAbsolutePath() + " Chunk " + searchChunk);
                if(snappyChunkRange == null) {
                    byte[] uncompressedBlock = getUncompressedBlock(searchChunk);
                    T firstInt = readFirstValue(uncompressedBlock);
                    T lastInt = readLastValue(uncompressedBlock);
                    snappyChunkRange = new BlockRange<T>(firstInt, lastInt);
                    PseudoCacheForSnappy.putSnappyChunkRange(indexFile, searchChunk, snappyChunkRange);
                } else {
                    log.trace("PseudoCacheForSnappy Cache Hit");
                }
                return (BlockRange<T>) snappyChunkRange;
            }
        };
        long currentChunk = integerOperationSearch.findFirstMatchingChunk(fileDataRetriever, queryValueRetriever, snappyChunks);
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
                        T currentValue = readValueFromDataInput(dis);
                        int fileNameHash = dis.readInt();
                        long offset = dis.readLong();
                        if(integerOperationSearch.matching(currentValue, queryValueRetriever)) {
                            result.add(new FileOffset(fileNameHash, offset, clause.getQueryClauses()));
                        } else if(!result.isEmpty()) {
                            // found some results, but here it isnt equal, that means end of results
                            return result;
                        }
                        if(queryLimit != -1 && queryLimit < result.size()) {
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

    public boolean acceptIndexFile(QueryClause queryClause, IF indexFile) {
        OperationSearch<T, IFV, IF> integerOperationSearch = OPERATIONS.get(queryClause.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + queryClause.getQueryOperation());
        }
        return integerOperationSearch.acceptIndexFile(integerOperationSearch.getQueryValueRetriever(queryClause), indexFile);
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
        SnappyChunksUtil.copy(dataInputStream, new File(absoluteImportPath), information.getFileLength(), getSnappyChunkSize());
    }

    public abstract Map<QueryOperation, OperationSearch<T, IFV, IF>> getQueryOperationsStrategies();
    public abstract int getSnappyChunkSize();
    public abstract T readValueFromDataInput(DataInput dis) throws IOException;
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

    protected CollectionDefinition getCollectionDefinition() {
        return collectionDefinition;
    }

    protected Map<IndexKey, List<IF>> getIndexFiles() {
        return indexFiles;
    }
}
