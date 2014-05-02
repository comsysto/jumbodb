package org.jumbodb.database.service.query.data.notfound;

import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class NotFoundDataStrategy implements DataStrategy {
    @Override
    public boolean isResponsibleFor(String collection, String chunkKey) {
        return true;
    }

    @Override
    public String getStrategyName() {
        return "NOT_FOUND";
    }

    @Override
    public int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        throw new IllegalStateException("No data strategy found for " + deliveryChunkDefinition.getCollection() + " " + deliveryChunkDefinition.getChunkKey());
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

    @Override
    public long getCompressedSize(File dataFolder) {
        // CARSTEN unit test
        return 0l;
    }

    @Override
    public long getUncompressedSize(File dataFolder) {
        // CARSTEN unit test
        return 0l;
    }
}
