package org.jumbodb.database.service.query.definition;

import java.io.File;
import java.util.List;
import java.util.Map;

public class DeliveryChunkDefinition {
    private String collection;
    private String chunkKey;
    private List<IndexDefinition> indexes;
    // CARSTEN do the same for datafiles  -> dataPath and strategies
    private Map<Integer, File> dataFiles;

    public DeliveryChunkDefinition(String collection, String chunkKey, List<IndexDefinition> indexes, Map<Integer, File> dataFiles) {
        this.collection = collection;
        this.chunkKey = chunkKey;
        this.indexes = indexes;
        this.dataFiles = dataFiles;
    }

    public List<IndexDefinition> getIndexes() {
        return indexes;
    }

    public Map<Integer, File> getDataFiles() {
        return dataFiles;
    }

    public String getChunkKey() {
        return chunkKey;
    }

    public String getCollection() {
        return collection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeliveryChunkDefinition that = (DeliveryChunkDefinition) o;

        if (chunkKey != null ? !chunkKey.equals(that.chunkKey) : that.chunkKey != null) return false;
        if (collection != null ? !collection.equals(that.collection) : that.collection != null) return false;
        if (dataFiles != null ? !dataFiles.equals(that.dataFiles) : that.dataFiles != null) return false;
        if (indexes != null ? !indexes.equals(that.indexes) : that.indexes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = collection != null ? collection.hashCode() : 0;
        result = 31 * result + (chunkKey != null ? chunkKey.hashCode() : 0);
        result = 31 * result + (indexes != null ? indexes.hashCode() : 0);
        result = 31 * result + (dataFiles != null ? dataFiles.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "DeliveryChunkDefinition{" +
                "collection='" + collection + '\'' +
                ", chunkKey='" + chunkKey + '\'' +
                ", indexes=" + indexes +
                ", dataFiles=" + dataFiles +
                '}';
    }
}