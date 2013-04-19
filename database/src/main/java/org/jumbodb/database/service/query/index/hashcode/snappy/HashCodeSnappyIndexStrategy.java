package org.jumbodb.database.service.query.index.hashcode.snappy;

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

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {

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
    public List<QueryOperation> getSupportedOperations() {
        return Arrays.asList(QueryOperation.EQ);
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
    }

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {
    }

    @Required
    public void setIndexFileExecutor(ExecutorService indexFileExecutor) {
        this.indexFileExecutor = indexFileExecutor;
    }
}
