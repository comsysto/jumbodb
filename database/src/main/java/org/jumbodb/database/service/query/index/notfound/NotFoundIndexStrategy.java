package org.jumbodb.database.service.query.index.notfound;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.index.IndexStrategy;

import java.io.File;
import java.io.InputStream;
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
        return "NOT_FOUND";
    }

    @Override
    public Set<FileOffset> findFileOffsets(String collection, String chunkKey, IndexQuery query, int queryLimit) {
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
    public void onImport(ImportMetaFileInformation information, InputStream dataInputStream, File absoluteImportPathFile) {
        throw new RuntimeException("Strategy " + information.getStrategy() + " was not found!");
    }
}
