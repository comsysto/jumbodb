package org.jumbodb.database.service.query.index;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.*;
import org.jumbodb.database.service.query.definition.CollectionDefinition;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

/**
 * User: carsten
 * Date: 4/19/13
 * Time: 1:07 PM
 */
public interface IndexStrategy {
    long getSize(File indexFolder);
    boolean isResponsibleFor(String chunkKey, String collection, String indexName);
    String getStrategyName();
    Set<FileOffset> findFileOffsets(String chunkKey, String collection, String indexName, List<IndexQuery> indexQueries, int queryLimit, boolean resultCacheEnabled);
    Set<QueryOperation> getSupportedOperations();
    void onInitialize(CollectionDefinition collectionDefinition);
    void onDataChanged(CollectionDefinition collectionDefinition);
}
