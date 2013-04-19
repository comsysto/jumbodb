package org.jumbodb.database.service.query.index.hashcode.snappy;

import java.io.File;

public class HashCodeSnappyIndexFile {
    private int fromHash;
    private int toHash;
    private File indexFile;

    public HashCodeSnappyIndexFile(int fromHash, int toHash, File indexFile) {
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

        HashCodeSnappyIndexFile hashCodeSnappyIndexFile1 = (HashCodeSnappyIndexFile) o;

        if (fromHash != hashCodeSnappyIndexFile1.fromHash) return false;
        if (toHash != hashCodeSnappyIndexFile1.toHash) return false;
        if (indexFile != null ? !indexFile.equals(hashCodeSnappyIndexFile1.indexFile) : hashCodeSnappyIndexFile1.indexFile != null) return false;

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
        return "HashCodeSnappyIndexFile{" +
                "fromHash=" + fromHash +
                ", toHash=" + toHash +
                ", indexFile=" + indexFile +
                '}';
    }
}