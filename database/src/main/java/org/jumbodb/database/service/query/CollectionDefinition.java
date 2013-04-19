package org.jumbodb.database.service.query;

import java.util.Collection;
import java.util.Collections;
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
        Collection<DeliveryChunkDefinition> deliveryChunkDefinitions = collections.get(collectionName);
        if(deliveryChunkDefinitions == null) {
            deliveryChunkDefinitions = Collections.emptyList();
        }
        return deliveryChunkDefinitions;
    }

    public DeliveryChunkDefinition getChunk(String collectionName, String chunkKey) {
        Collection<DeliveryChunkDefinition> chunks = getChunks(collectionName);
        for (DeliveryChunkDefinition chunk : chunks) {
            if(chunk.getChunkKey().equals(chunkKey)) {
                return chunk;
            }
        }
        return null;
    }

    public IndexDefinition getChunkIndex(String collectionName, String chunkKey, String indexName) {
        DeliveryChunkDefinition chunk = getChunk(collectionName, chunkKey);
        if(chunk != null) {
            for (IndexDefinition indexDefinition : chunk.getIndexes()) {
                if(indexName.equals(indexDefinition.getName())) {
                    return indexDefinition;
                }
            }
        }
        return null;
    }
}
