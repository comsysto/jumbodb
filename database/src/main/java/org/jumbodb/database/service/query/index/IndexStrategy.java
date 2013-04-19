package org.jumbodb.database.service.query.index;

import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.JumboQuery;
import org.jumbodb.database.service.query.QueryOperation;

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
    Set<FileOffset> findFileOffsets(String collection, String chunkKey, JumboQuery.IndexQuery query);
    List<QueryOperation> getSupportedOperations();
    void onDataChanged();
}
