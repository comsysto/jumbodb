package org.jumbodb.database.service.query.index.hashcode.snappy;

import org.jumbodb.connector.query.JumboQuery;
import org.jumbodb.database.service.query.*;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.springframework.beans.factory.annotation.Required;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * @author Carsten Hufe
 */
public class HashCodeSnappyIndexStrategy implements IndexStrategy {

    private ExecutorService indexFileExecutor;
    private CollectionDefinition collectionDefinition;

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {
        IndexDefinition chunkIndex = collectionDefinition.getChunkIndex(collection, chunkKey, indexName);
        if(chunkIndex != null) {
            return getStrategyName().equals(chunkIndex.getStrategy());
        }
        return false;
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE_SNAPPY_V1";
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, JumboQuery.IndexQuery query) {
        return null;
    }

    @Override
    public List<JumboQuery.QueryOperation> getSupportedOperations() {
        return Arrays.asList(JumboQuery.QueryOperation.EQ);
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
    public void setIndexFileExecutor(ExecutorService indexFileExecutor) {
        this.indexFileExecutor = indexFileExecutor;
    }
}
