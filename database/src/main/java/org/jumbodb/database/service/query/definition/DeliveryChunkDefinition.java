package org.jumbodb.database.service.query.definition;

import java.io.File;
import java.util.List;
import java.util.Map;

public class DeliveryChunkDefinition {
    private String collection;
    private String chunkKey;
    private String dateFormat;
    private List<IndexDefinition> indexes;
    private Map<Integer, File> dataFiles;
    private String dataStrategy;

    public DeliveryChunkDefinition(String chunkKey, String collection, String dateFormat, List<IndexDefinition> indexes, Map<Integer, File> dataFiles, String dataStrategy) {
        this.collection = collection;
        this.chunkKey = chunkKey;
        this.dateFormat = dateFormat;
        this.indexes = indexes;
        this.dataFiles = dataFiles;
        this.dataStrategy = dataStrategy;
    }

    public String getDateFormat() {
        return dateFormat;
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

    public String getDataStrategy() {
        return dataStrategy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeliveryChunkDefinition that = (DeliveryChunkDefinition) o;

        if (chunkKey != null ? !chunkKey.equals(that.chunkKey) : that.chunkKey != null) return false;
        if (collection != null ? !collection.equals(that.collection) : that.collection != null) return false;
        if (dataFiles != null ? !dataFiles.equals(that.dataFiles) : that.dataFiles != null) return false;
        if (dataStrategy != null ? !dataStrategy.equals(that.dataStrategy) : that.dataStrategy != null) return false;
        if (indexes != null ? !indexes.equals(that.indexes) : that.indexes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = collection != null ? collection.hashCode() : 0;
        result = 31 * result + (chunkKey != null ? chunkKey.hashCode() : 0);
        result = 31 * result + (indexes != null ? indexes.hashCode() : 0);
        result = 31 * result + (dataFiles != null ? dataFiles.hashCode() : 0);
        result = 31 * result + (dataStrategy != null ? dataStrategy.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "DeliveryChunkDefinition{" +
                "collection='" + collection + '\'' +
                ", chunkKey='" + chunkKey + '\'' +
                ", indexes=" + indexes +
                ", dataFiles=" + dataFiles +
                ", dataStrategy='" + dataStrategy + '\'' +
                '}';
    }
}