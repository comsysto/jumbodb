package org.jumbodb.database.service.query.index.snappy;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.compression.CompressionBlocksUtil;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.jumbodb.database.service.query.index.IndexKey;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.index.common.BlockRange;
import org.jumbodb.database.service.query.index.common.CacheIndexClause;
import org.jumbodb.database.service.query.index.common.ChunkRangeKey;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
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
public abstract class NumberSnappyIndexStrategy<T, IFV, IF extends NumberIndexFile<IFV>> implements IndexStrategy {

    private Logger log = LoggerFactory.getLogger(NumberSnappyIndexStrategy.class);

    private ExecutorService indexFileExecutor;
    private CollectionDefinition collectionDefinition;
    private Map<IndexKey, List<IF>> indexFiles;
    private Cache indexCompressionBlocksCache;
    private Cache indexBlockRangesCache;
    private Cache indexQueryCache;

    protected final Map<QueryOperation, IndexOperationSearch<T, IFV, IF>> OPERATIONS = createOperations();

    private Map<QueryOperation, IndexOperationSearch<T, IFV, IF>> createOperations() {
        return getQueryOperationsStrategies();
    }

    private Blocks getCompressionBlocksByFile(File file) {
        Cache.ValueWrapper valueWrapper = indexCompressionBlocksCache.get(file);
        if(valueWrapper != null) {
            return (Blocks) valueWrapper.get();
        }
        Blocks blocksByFile = CompressionBlocksUtil.getBlocksByFile(file);
        indexCompressionBlocksCache.put(file, blocksByFile);
        return blocksByFile;
    }

    public Set<FileOffset> searchOffsetsByIndexQueries(File indexFile, Set<IndexQuery> indexQueries, int queryLimit, boolean resultCacheEnabled) throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = null;
        List<FileOffset> result = new LinkedList<FileOffset>();
        try {
            Blocks blocks = getCompressionBlocksByFile(indexFile);
            raf = new RandomAccessFile(indexFile, "r");

            for (IndexQuery indexQuery : indexQueries) {
                if(queryLimit == -1 || queryLimit > result.size()) {
                    result.addAll(findOffsetForIndexQuery(indexFile, raf, indexQuery, blocks, queryLimit, resultCacheEnabled));
                }
            }

        } finally {
            IOUtils.closeQuietly(raf);
        }
        log.trace("Search one index part-file with " + result.size() + " offsets in " + (System.currentTimeMillis() - start) + "ms");
        return new HashSet<FileOffset>(result);
    }

    @Override
    public boolean isResponsibleFor(String chunkKey, String collection, String indexName) {
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
                        IndexKey indexKey = new IndexKey(deliveryChunkDefinition.getChunkKey(), collection, indexDefinition.getName());
                        result.put(indexKey, buildIndexRange(indexDefinition.getPath()));
                    }
                }
            }
        }
        return result;
    }

    protected List<IF> buildIndexRange(File indexFolder) {
        List<IF> result = new LinkedList<IF>();
        File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".idx"));
        for (File indexFile : indexFiles) {
            Blocks blocks = getCompressionBlocksByFile(indexFile);
            if(blocks.getNumberOfBlocks() > 0) {
                result.add(createIndexFileDescription(indexFile, blocks));
            }
        }
        return result;
    }

    protected IF createIndexFileDescription(File indexFile, Blocks blocks) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(indexFile, "r");
            byte[] uncompressed = SnappyUtil.getUncompressed(raf, blocks, 0);
            T from = readFirstValue(uncompressed);
            uncompressed = SnappyUtil.getUncompressed(raf, blocks, blocks.getNumberOfBlocks() - 1);
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
    public Set<FileOffset> findFileOffsets(String chunkKey, String collection, String indexName, List<IndexQuery> indexQueries, int queryLimit, boolean resultCacheEnabled) {
        try {
            MultiValueMap<File, IndexQuery> groupedByIndexFile = groupByIndexFile(chunkKey, collection, indexName, indexQueries);
            List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();

            for (Map.Entry<File, List<IndexQuery>> clausesPerIndexFile : groupedByIndexFile.entrySet()) {
                tasks.add(indexFileExecutor.submit(new NumberSnappyIndexTask(this, clausesPerIndexFile.getKey(),
                        new HashSet<IndexQuery>(clausesPerIndexFile.getValue()), queryLimit, resultCacheEnabled)));
            }
            Set<FileOffset> result = new HashSet<FileOffset>();
            for (Future<Set<FileOffset>> task : tasks) {
                result.addAll(task.get());
            }
            return result;
        } catch(ExecutionException e) {
            throw (RuntimeException)e.getCause();
        } catch (InterruptedException e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public long getSize(File indexFolder) {
        return FileUtils.sizeOfDirectory(indexFolder);
    }

    public MultiValueMap<File, IndexQuery> groupByIndexFile(String chunkKey, String collection, String indexName, List<IndexQuery> indexQueries) {
        List<IF> indexFiles = getIndexFiles(chunkKey, collection, indexName);
        MultiValueMap<File, IndexQuery> groupByIndexFile = new LinkedMultiValueMap<File, IndexQuery>();
        for (IF indexFile : indexFiles) {
            for (IndexQuery indexQuery : indexQueries) {
                if(acceptIndexFile(indexQuery, indexFile)) {
                    groupByIndexFile.add(indexFile.getIndexFile(), indexQuery);
                }
            }
        }
        return groupByIndexFile;
    }

    protected List<IF> getIndexFiles(String chunkKey, String collection, String indexName) {
        return indexFiles.get(new IndexKey(chunkKey, collection, indexName));
    }


    protected Set<FileOffset> findOffsetForIndexQuery(File indexFile, RandomAccessFile indexRaf, IndexQuery indexQuery,
                                                      Blocks blocks, int queryLimit, boolean resultCacheEnabled) throws IOException {
        if(resultCacheEnabled) {
            CacheIndexClause key = new CacheIndexClause(indexFile, indexQuery.getQueryOperation(), indexQuery.getValue());
            Cache.ValueWrapper valueWrapper = indexQueryCache.get(key);
            if(valueWrapper != null) {
                return (Set<FileOffset>) valueWrapper.get();
            }
            Set<FileOffset> fileOffsets = getFileOffsets(indexFile, indexRaf, indexQuery, blocks, queryLimit);
            indexQueryCache.put(key, fileOffsets);
            return fileOffsets;

        } else {
            return getFileOffsets(indexFile, indexRaf, indexQuery, blocks, queryLimit);
        }
    }

    private Set<FileOffset> getFileOffsets(final File indexFile, final RandomAccessFile indexRaf, IndexQuery indexQuery, final Blocks blocks, int queryLimit) throws IOException {
        IndexOperationSearch<T, IFV, IF> integerOperationSearch = OPERATIONS.get(indexQuery.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + indexQuery.getQueryOperation());
        }
        QueryValueRetriever queryValueRetriever = integerOperationSearch.getQueryValueRetriever(indexQuery);
        FileDataRetriever fileDataRetriever = new FileDataRetriever() {

            public byte[] getUncompressedBlock(long searchChunk) throws IOException {
                return SnappyUtil.getUncompressed(indexRaf, blocks, searchChunk);
            }

            @Override
            public BlockRange<T> getBlockRange(long searchChunk) throws IOException {
                ChunkRangeKey chunkRangeKey = new ChunkRangeKey(indexFile, searchChunk);
                Cache.ValueWrapper valueWrapper = indexBlockRangesCache.get(chunkRangeKey);
                if(valueWrapper == null) {
                    byte[] uncompressedBlock = getUncompressedBlock(searchChunk);
                    T firstInt = readFirstValue(uncompressedBlock);
                    T lastInt = readLastValue(uncompressedBlock);
                    BlockRange<T> snappyChunkRange = new BlockRange<T>(firstInt, lastInt);
                    indexBlockRangesCache.put(chunkRangeKey, snappyChunkRange);
                    return snappyChunkRange;
                }
                else {
                    return (BlockRange<T>) valueWrapper.get();
                }
            }
        };
//        long start = System.currentTimeMillis();
        long currentChunk = integerOperationSearch.findFirstMatchingChunk(fileDataRetriever, queryValueRetriever, blocks);
        long numberOfChunks = blocks.getNumberOfBlocks();
//        log.trace("findFirstMatchingChunk currentChunk=" + currentChunk + "/" + numberOfChunks  + " took " + (System.currentTimeMillis() - start) + "ms");

        int checkedChunks = 0;
        int matchedChunks = 0;
        if(currentChunk >= 0) {
            FileInputStream fis = null;
            BufferedInputStream bis = null;
            DataInputStream dis = null;
            try {
                fis = new FileInputStream(indexFile);
                bis = new BufferedInputStream(fis);
                dis = new DataInputStream(bis);
                // CARSTEN attention getOffsetfor is snappy specific +4
                dis.skip(blocks.getOffsetForBlock(currentChunk));
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
                            if(integerOperationSearch.matchingChunk(currentValue, queryValueRetriever)) {
                                matchedChunks++;
                            }
                            if(integerOperationSearch.matching(currentValue, queryValueRetriever)) {
                                result.add(new FileOffset(fileNameHash, offset, indexQuery));
                            }
                            else if(integerOperationSearch.searchFinished(currentValue, queryValueRetriever, result.size() > 0)) {
//                            else if(!result.isEmpty()) {
                                // found some results, but here it isnt equal, that means end of results
                                return result;
                            }
                            if(queryLimit != -1 && queryLimit < result.size()) {
                                return result;
                            }
                        }
                        // nothing found in second block, so there isn't anything
                        if(checkedChunks > 1 && matchedChunks == 0) {
                        //    System.out.println("blub" + checkedChunks);
                            return result;
                        }
                    } finally {
                        IOUtils.closeQuietly(byteDis);
                        IOUtils.closeQuietly(bais);
                    }

                    currentChunk++;
                    checkedChunks++;
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


    public boolean acceptIndexFile(IndexQuery indexQuery, IF indexFile) {
        IndexOperationSearch<T, IFV, IF> integerOperationSearch = OPERATIONS.get(indexQuery.getQueryOperation());
        if(integerOperationSearch == null) {
            throw new UnsupportedOperationException("QueryOperation is not supported: " + indexQuery.getQueryOperation());
        }
        return integerOperationSearch.acceptIndexFile(integerOperationSearch.getQueryValueRetriever(indexQuery), indexFile);
    }

    @Override
    public Set<QueryOperation> getSupportedOperations() {
        return OPERATIONS.keySet();
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
        onDataChanged(collectionDefinition);
    }

    public abstract Map<QueryOperation, IndexOperationSearch<T, IFV, IF>> getQueryOperationsStrategies();
    public abstract int getCompressionBlockSize();
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
    public void setIndexCompressionBlocksCache(Cache indexCompressionBlocksCache) {
        this.indexCompressionBlocksCache = indexCompressionBlocksCache;
    }

    @Required
    public void setIndexBlockRangesCache(Cache indexBlockRangesCache) {
        this.indexBlockRangesCache = indexBlockRangesCache;
    }

    @Required
    public void setIndexQueryCache(Cache indexQueryCache) {
        this.indexQueryCache = indexQueryCache;
    }

    protected CollectionDefinition getCollectionDefinition() {
        return collectionDefinition;
    }

    protected Map<IndexKey, List<IF>> getIndexFiles() {
        return indexFiles;
    }
}
