package org.jumbodb.database.service.query;

import java.io.File;

public class IndexFile {
    private int fromHash;
    private int toHash;
    private File indexFile;

    public IndexFile(int fromHash, int toHash, File indexFile) {
        this.fromHash = fromHash;
        this.toHash = toHash;
        this.indexFile = indexFile;
    }

    public int getFromHash() {
        return fromHash;
    }

    public int getToHash() {
        return toHash;
    }

    public File getIndexFile() {
        return indexFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexFile indexFile1 = (IndexFile) o;

        if (fromHash != indexFile1.fromHash) return false;
        if (toHash != indexFile1.toHash) return false;
        if (indexFile != null ? !indexFile.equals(indexFile1.indexFile) : indexFile1.indexFile != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fromHash;
        result = 31 * result + toHash;
        result = 31 * result + (indexFile != null ? indexFile.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IndexFile{" +
                "fromHash=" + fromHash +
                ", toHash=" + toHash +
                ", indexFile=" + indexFile +
                '}';
    }
}