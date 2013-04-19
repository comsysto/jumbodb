package org.jumbodb.database.service.query;

import java.util.Collection;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class CollectionDefinition {
    private Map<String, Collection<DeliveryChunkDefinition>> collections;

    public CollectionDefinition(Map<String, Collection<DeliveryChunkDefinition>> collections) {
        this.collections = collections;
    }

    public Collection<String> getCollections() {
        return collections.keySet();
    }

    public Collection<DeliveryChunkDefinition> getChunks(String collectionName) {
        return collections.get(collectionName);
    }
}
