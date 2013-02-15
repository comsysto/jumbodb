package core.query;

import java.io.File;
import java.util.Collection;
import java.util.Map;

public class DataCollection {
    private Map<String, Collection<IndexFile>> indexFiles;
    private Map<Integer, File> dataFiles;

    public DataCollection(Map<String, Collection<IndexFile>> indexFiles, Map<Integer, File> dataFiles) {
        this.indexFiles = indexFiles;
        this.dataFiles = dataFiles;
    }

    public Map<String, Collection<IndexFile>> getIndexFiles() {
        return indexFiles;
    }

    public Map<Integer, File> getDataFiles() {
        return dataFiles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataCollection that = (DataCollection) o;

        if (dataFiles != null ? !dataFiles.equals(that.dataFiles) : that.dataFiles != null) return false;
        if (indexFiles != null ? !indexFiles.equals(that.indexFiles) : that.indexFiles != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = indexFiles != null ? indexFiles.hashCode() : 0;
        result = 31 * result + (dataFiles != null ? dataFiles.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DataCollection{" +
                "indexFiles=" + indexFiles +
                ", dataFiles=" + dataFiles +
                '}';
    }
}