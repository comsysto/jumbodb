package org.jumbodb.database.service.query.index.notfound;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.index.IndexStrategy;

import java.io.File;
import java.util.Collections;
import java.util.Set;

/**
 * @author Carsten Hufe
 */
public class NotFoundIndexStrategy implements IndexStrategy {
    @Override
    public boolean isResponsibleFor(String chunkKey, String collection, String indexName) {
        return true;
    }

    @Override
    public String getStrategyName() {
        return "NOT_FOUND";
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query, int queryLimit, boolean resultCacheEnabled) {
        throw new IllegalStateException("No index strategy found for " + collection + " " + chunkKey);
    }

    @Override
    public Set<QueryOperation> getSupportedOperations() {
        return Collections.emptySet();
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {

    }

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {

    }

    @Override
    public long getSize(File indexFolder) {
        return 0l;
    }
}
