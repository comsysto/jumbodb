package org.jumbodb.database.service.query;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class DataDeliveryChunk {
    private String collection;
    private String chunkKey;
    private Map<String, Collection<IndexFile>> indexFiles;
    private Map<Integer, File> dataFiles;

    public DataDeliveryChunk(String collection, String chunkKey, Map<String, Collection<IndexFile>> indexFiles, Map<Integer, File> dataFiles) {
        this.collection = collection;
        this.chunkKey = chunkKey;
        this.indexFiles = indexFiles;
        this.dataFiles = dataFiles;
    }

    public Map<String, Collection<IndexFile>> getIndexFiles() {
        return indexFiles;
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

        DataDeliveryChunk that = (DataDeliveryChunk) o;

        if (chunkKey != null ? !chunkKey.equals(that.chunkKey) : that.chunkKey != null) return false;
        if (collection != null ? !collection.equals(that.collection) : that.collection != null) return false;
        if (dataFiles != null ? !dataFiles.equals(that.dataFiles) : that.dataFiles != null) return false;
        if (indexFiles != null ? !indexFiles.equals(that.indexFiles) : that.indexFiles != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = collection != null ? collection.hashCode() : 0;
        result = 31 * result + (chunkKey != null ? chunkKey.hashCode() : 0);
        result = 31 * result + (indexFiles != null ? indexFiles.hashCode() : 0);
        result = 31 * result + (dataFiles != null ? dataFiles.hashCode() : 0);
        return result;
    }


    @Override
    public String toString() {
        return "DataDeliveryChunk{" +
                "collection='" + collection + '\'' +
                ", chunkKey='" + chunkKey + '\'' +
                ", indexFiles=" + indexFiles +
                ", dataFiles=" + dataFiles +
                '}';
    }
}