package org.jumbodb.database.service.query.data;

import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;

import java.io.File;
import java.util.Collection;
import java.util.List;

/**
 * User: carsten
 * Date: 4/19/13
 * Time: 1:07 PM
 */
public interface DataStrategy {
    CollectionDataSize getCollectionDataSize(File dataFolder);
    boolean isResponsibleFor(String chunkKey, String collection);
    String getStrategyName();
    boolean matches(QueryOperation operation, Object leftValue, Object rightValue);
    int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery);
    List<QueryOperation> getSupportedOperations();
    void onInitialize(CollectionDefinition collectionDefinition);
    void onDataChanged(CollectionDefinition collectionDefinition);
}
