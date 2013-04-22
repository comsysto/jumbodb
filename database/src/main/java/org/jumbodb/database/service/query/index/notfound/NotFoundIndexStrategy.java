package org.jumbodb.database.service.query.index.notfound;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.index.IndexStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Carsten Hufe
 */
public class NotFoundIndexStrategy implements IndexStrategy {
    @Override
    public boolean isResponsibleFor(String collection, String chunkKey, String indexName) {
        return true;
    }

    @Override
    public String getStrategyName() {
        return "NOT_FOUND";  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query) {
        throw new IllegalStateException("No index strategy found for " + collection + " " + chunkKey);
    }

    @Override
    public List<QueryOperation> getSupportedOperations() {
        return Collections.emptyList();
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {

    }

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {

    }
}
