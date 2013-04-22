package org.jumbodb.database.service.query.data;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;

import java.util.List;
import java.util.Set;

/**
 * User: carsten
 * Date: 4/19/13
 * Time: 1:07 PM
 */
public interface DataStrategy {
    boolean isResponsibleFor(String collection, String chunkKey);
    String getStrategyName();
    Set<FileOffset> findFileOffsets(String collection, String chunkKey, Set<FileOffset> fileOffsets, JumboQuery jumboQuery);
    List<QueryOperation> getSupportedOperations();
    void onInitialize(CollectionDefinition collectionDefinition);
    void onDataChanged(CollectionDefinition collectionDefinition);
}
