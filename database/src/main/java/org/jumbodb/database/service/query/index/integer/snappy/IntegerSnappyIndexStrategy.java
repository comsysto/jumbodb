package org.jumbodb.database.service.query.index.integer.snappy;

import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.NotImplementedException;
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
import org.jumbodb.database.service.query.index.hashcode.snappy.HashCodeSnappyIndexFile;
import org.jumbodb.database.service.query.index.hashcode.snappy.HashCodeSnappyIndexTask;
import org.jumbodb.database.service.query.index.hashcode.snappy.HashCodeSnappySearchIndexUtils;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyChunksUtil;
import org.jumbodb.database.service.query.snappy.SnappyStreamToFileCopy;
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
public class IntegerSnappyIndexStrategy implements IndexStrategy {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte data hash, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(IntegerSnappyIndexStrategy.class);

    private ExecutorService indexFileExecutor;
    private CollectionDefinition collectionDefinition;
    private Map<IndexKey, List<IntegerSnappyIndexFile>> indexFiles;

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {
        IndexDefinition chunkIndex = collectionDefinition.getChunkIndex(collection, chunkKey, indexName);
        if(chunkIndex != null) {
            return getStrategyName().equals(chunkIndex.getStrategy());
        }
        return false;
    }

    private Map<IndexKey, List<IntegerSnappyIndexFile>> buildIndexRanges() {
        Map<IndexKey, List<IntegerSnappyIndexFile>> result = Maps.newHashMap();
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

    private List<IntegerSnappyIndexFile> buildIndexRange(File indexFolder) {
        List<IntegerSnappyIndexFile> result = new LinkedList<IntegerSnappyIndexFile>();
        File[] indexFiles = indexFolder.listFiles((FilenameFilter) new SuffixFileFilter(".odx"));
        for (File indexFile : indexFiles) {
                SnappyChunks snappyChunks = SnappyChunksUtil.getSnappyChunksByFile(indexFile);
                if(snappyChunks.getNumberOfChunks() > 0) {
                    result.add(createIndexFileDescription(indexFile, snappyChunks));
                }
        }
        return result;
    }

    private IntegerSnappyIndexFile createIndexFileDescription(File indexFile, SnappyChunks snappyChunks) {
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(indexFile, "r");
            // CARSTEN fix use other libraries
            byte[] uncompressed = HashCodeSnappySearchIndexUtils.getUncompressed(raf, snappyChunks, 0);
            int fromHash = HashCodeSnappySearchIndexUtils.readFirstHash(uncompressed);
            uncompressed = HashCodeSnappySearchIndexUtils.getUncompressed(raf, snappyChunks, snappyChunks.getNumberOfChunks() - 1);
            int toHash = HashCodeSnappySearchIndexUtils.readLastHash(uncompressed);
            return new IntegerSnappyIndexFile(fromHash, toHash, indexFile);
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
        return "INTEGER_SNAPPY_V1";
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query) {
        try {
            MultiValueMap<File, QueryClause> groupedByIndexFile = groupByIndexFile(collection, chunkKey, query);
            List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
            for (File indexFile : groupedByIndexFile.keySet()) {
                tasks.add(indexFileExecutor.submit(new IntegerSnappyIndexTask(indexFile, new HashSet<QueryClause>(groupedByIndexFile.get(indexFile)))));
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
        List<IntegerSnappyIndexFile> indexFiles = getIndexFiles(collection, chunkKey, query);
        MultiValueMap<File, QueryClause> groupByIndexFile = new LinkedMultiValueMap<File, QueryClause>();
        for (IntegerSnappyIndexFile hashCodeSnappyIndexFile : indexFiles) {
            for (QueryClause obj : query.getClauses()) {
                int intValue = (Integer)obj.getValue();
                if(QueryOperation.EQ == obj.getQueryOperation()) {
                    if (intValue >= hashCodeSnappyIndexFile.getFromInt() && intValue <= hashCodeSnappyIndexFile.getToInt()) {
                        groupByIndexFile.add(hashCodeSnappyIndexFile.getIndexFile(), obj);
                    }
                }
                else if(QueryOperation.BETWEEN == obj.getQueryOperation()) {
                    throw new NotImplementedException("Not yet implemented");
                }
                else if(QueryOperation.GT == obj.getQueryOperation()) {
                    throw new NotImplementedException("Not yet implemented");
                }
                else if(QueryOperation.LT == obj.getQueryOperation()) {
                    throw new NotImplementedException("Not yet implemented");
                }
                else if(QueryOperation.NE == obj.getQueryOperation()) {
                    throw new NotImplementedException("Not yet implemented");
                }
            }
        }
        return groupByIndexFile;
    }

    private List<IntegerSnappyIndexFile> getIndexFiles(String collection, String chunkKey, IndexQuery query) {
        return indexFiles.get(new IndexKey(collection, chunkKey, query.getName()));
    }

    @Override
    public List<QueryOperation> getSupportedOperations() {
        return Arrays.asList(QueryOperation.EQ, QueryOperation.BETWEEN, QueryOperation.NE, QueryOperation.GT, QueryOperation.LT);
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
        onDataChanged(collectionDefinition);
    }

    @Override
    public void onImport(ImportMetaFileInformation information, InputStream dataInputStream, File absoluteImportPathFile) {
        String absoluteImportPath = absoluteImportPathFile.getAbsolutePath() + "/" + information.getFileName();
        SnappyStreamToFileCopy.copy(dataInputStream, new File(absoluteImportPath), information.getFileLength(), SNAPPY_INDEX_CHUNK_SIZE);

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
