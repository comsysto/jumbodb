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
import org.springframework.cache.Cache;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.xerial.snappy.Snappy;

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
    private Cache indexSnappyChunksCache;
    private Cache indexBlockRangesCache;

    protected final Map<QueryOperation, OperationSearch<T, IFV, IF>> OPERATIONS = createOperations();

    private Map<QueryOperation, OperationSearch<T, IFV, IF>> createOperations() {
        return getQueryOperationsStrategies();
    }

    private SnappyChunks getSnappyChunksByFile(File file) {
        Cache.ValueWrapper valueWrapper = indexSnappyChunksCache.get(file);
        if(valueWrapper != null) {
            return (SnappyChunks) valueWrapper.get();
        }
        SnappyChunks snappyChunksByFile = SnappyChunksUtil.getSnappyChunksByFile(file);
        indexSnappyChunksCache.put(file, snappyChunksByFile);
        return snappyChunksByFile;
    }

    public Set<FileOffset> searchOffsetsByClauses(File indexFile, Set<QueryClause> clauses, int queryLimit) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        List<FileOffset> result = new LinkedList<FileOffset>();
        try {
            SnappyChunks snappyChunks = getSnappyChunksByFile(indexFile);
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
            SnappyChunks snappyChunks = getSnappyChunksByFile(indexFile);
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
                Cache.ValueWrapper valueWrapper = indexBlockRangesCache.get(new ChunkRangeKey(indexFile, searchChunk));
                if(valueWrapper == null) {
                    byte[] uncompressedBlock = getUncompressedBlock(searchChunk);
                    T firstInt = readFirstValue(uncompressedBlock);
                    T lastInt = readLastValue(uncompressedBlock);
                    BlockRange<T> snappyChunkRange = new BlockRange<T>(firstInt, lastInt);
                    indexBlockRangesCache.put(new ChunkRangeKey(indexFile, searchChunk), snappyChunkRange);
                    return snappyChunkRange;
                }
                else {
                    return (BlockRange<T>) valueWrapper.get();
                }
            }
        };
//        long start = System.currentTimeMillis();
        long currentChunk = integerOperationSearch.findFirstMatchingChunk(fileDataRetriever, queryValueRetriever, snappyChunks);
        long numberOfChunks = snappyChunks.getNumberOfChunks();
//        log.trace("findFirstMatchingChunk currentChunk=" + currentChunk + "/" + numberOfChunks  + " took " + (System.currentTimeMillis() - start) + "ms");

        int i = 0;
        if(currentChunk >= 0) {
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            DataInputStream dis = null;
            try {
                fis = new FileInputStream(indexFile);
                bis = new BufferedInputStream(fis);
                dis = new DataInputStream(bis);
                dis.skip(snappyChunks.getOffsetForChunk(currentChunk));
                Set<FileOffset> result = new HashSet<FileOffset>();
                while(currentChunk < numberOfChunks) {
                    int compressedSize = dis.readInt();
                    byte[] compressed = new byte[compressedSize];
                    dis.read(compressed);
                    byte[] uncompressed =  Snappy.uncompress(compressed);
//                    byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
                    ByteArrayInputStream bais = null;
                    DataInputStream byteDis = null;
                    try {
                        bais = new ByteArrayInputStream(uncompressed);
                        byteDis = new DataInputStream(bais);
                        while(bais.available() > 0) {
                            T currentValue = readValueFromDataInput(byteDis);
                            int fileNameHash = byteDis.readInt();
                            long offset = byteDis.readLong();
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
                        // nothing found in second block, so there isn't anything
                        if(i > 1 && result.isEmpty()) {
                            return result;
                        }
                    } finally {
                        IOUtils.closeQuietly(byteDis);
                        IOUtils.closeQuietly(bais);
                    }

                    currentChunk++;
                    i++;
                }
                return result;
            } finally {
                IOUtils.closeQuietly(dis);
                IOUtils.closeQuietly(bis);
                IOUtils.closeQuietly(fis);
            }
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

    @Required
    public void setIndexSnappyChunksCache(Cache indexSnappyChunksCache) {
        this.indexSnappyChunksCache = indexSnappyChunksCache;
    }

    @Required
    public void setIndexBlockRangesCache(Cache indexBlockRangesCache) {
        this.indexBlockRangesCache = indexBlockRangesCache;
    }

    protected CollectionDefinition getCollectionDefinition() {
        return collectionDefinition;
    }

    protected Map<IndexKey, List<IF>> getIndexFiles() {
        return indexFiles;
    }
}
