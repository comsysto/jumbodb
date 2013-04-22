package org.jumbodb.database.service.query.index;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.*;
import org.jumbodb.database.service.query.definition.CollectionDefinition;

import java.util.List;
import java.util.Set;

/**
 * User: carsten
 * Date: 4/19/13
 * Time: 1:07 PM
 */
public interface IndexStrategy {
    boolean isResponsibleFor(String collection, String chunkKey, String indexName);
    String getStrategyName();
    Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query);
    List<QueryOperation> getSupportedOperations();
    void onInitialize(CollectionDefinition collectionDefinition);
    void onDataChanged(CollectionDefinition collectionDefinition);
}