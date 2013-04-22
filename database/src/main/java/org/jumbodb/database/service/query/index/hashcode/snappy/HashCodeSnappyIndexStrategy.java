package org.jumbodb.database.service.query.index.hashcode.snappy;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.*;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.jumbodb.database.service.query.definition.IndexDefinition;
import org.jumbodb.database.service.query.index.IndexKey;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil;
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
public class HashCodeSnappyIndexStrategy implements IndexStrategy {

    private ExecutorService indexFileExecutor;
    private CollectionDefinition collectionDefinition;
    private Map<IndexKey, List<HashCodeSnappyIndexFile>> indexFiles;

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {
        IndexDefinition chunkIndex = collectionDefinition.getChunkIndex(collection, chunkKey, indexName);
        if(chunkIndex != null) {
            return getStrategyName().equals(chunkIndex.getStrategy());
        }
        return false;
    }

    private Map<IndexKey, List<HashCodeSnappyIndexFile>> buildIndexRanges() {
        Map<IndexKey, List<HashCodeSnappyIndexFile>> result = Maps.newHashMap();
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

    private List<HashCodeSnappyIndexFile> buildIndexRange(File indexFolder) {
        List<HashCodeSnappyIndexFile> result = new LinkedList<HashCodeSnappyIndexFile>();
        File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".odx"));
        for (File indexFile : indexFiles) {
                SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(indexFile);
                if(snappyChunks.getNumberOfChunks() > 0) {
                    result.add(createIndexFileDescription(indexFile, snappyChunks));
                }
        }
        return result;
    }

    private HashCodeSnappyIndexFile createIndexFileDescription(File indexFile, SnappyChunks snappyChunks) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(indexFile, "r");
            byte[] uncompressed = HashCodeSnappySearchIndexUtils.getUncompressed(raf, snappyChunks, 0);
            int fromHash = HashCodeSnappySearchIndexUtils.readFirstHash(uncompressed);
            uncompressed = HashCodeSnappySearchIndexUtils.getUncompressed(raf, snappyChunks, snappyChunks.getNumberOfChunks() - 1);
            int toHash = HashCodeSnappySearchIndexUtils.readLastHash(uncompressed);
            return new HashCodeSnappyIndexFile(fromHash, toHash, indexFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(raf);
        }
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE_SNAPPY_V1";
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query) {
        try {
            MultiValueMap<File, Integer> groupedByIndexFile = groupByIndexFile(collection, chunkKey, query);
            List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
            for (File indexFile : groupedByIndexFile.keySet()) {
                tasks.add(indexFileExecutor.submit(new HashCodeSnappyIndexTask(indexFile, new HashSet<Integer>(groupedByIndexFile.get(indexFile)))));
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

    public MultiValueMap<File, Integer> groupByIndexFile(String collection, String chunkKey, IndexQuery query) {
        List<HashCodeSnappyIndexFile> indexFiles = getIndexFiles(collection, chunkKey, query);
        MultiValueMap<File, Integer> groupByIndexFile = new LinkedMultiValueMap<File, Integer>();
        for (HashCodeSnappyIndexFile hashCodeSnappyIndexFile : indexFiles) {
            for (QueryClause obj : query.getClauses()) {
                int hash = obj.getValue().hashCode();
                if (hash >= hashCodeSnappyIndexFile.getFromHash() && hash <= hashCodeSnappyIndexFile.getToHash()) {
                    groupByIndexFile.add(hashCodeSnappyIndexFile.getIndexFile(), hash);
                }

            }
        }
        return groupByIndexFile;
    }

    private List<HashCodeSnappyIndexFile> getIndexFiles(String collection, String chunkKey, IndexQuery query) {
        return indexFiles.get(new IndexKey(collection, chunkKey, query.getName()));
    }

    @Override
    public List<QueryOperation> getSupportedOperations() {
        return Arrays.asList(QueryOperation.EQ);
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
        onDataChanged(collectionDefinition);
    }

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
