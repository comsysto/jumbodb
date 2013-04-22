package org.jumbodb.database.service.query.index.integer.snappy;

import java.io.File;

public class IntegerSnappyIndexFile {
    private int fromInt;
    private int toInt;
    private File indexFile;

    public IntegerSnappyIndexFile(int fromInt, int toInt, File indexFile) {
        this.fromInt = fromInt;
        this.toInt = toInt;
        this.indexFile = indexFile;
    }

    public int getFromInt() {
        return fromInt;
    }

    public int getToInt() {
        return toInt;
    }

    public File getIndexFile() {
        return indexFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntegerSnappyIndexFile that = (IntegerSnappyIndexFile) o;

        if (fromInt != that.fromInt) return false;
        if (toInt != that.toInt) return false;
        if (indexFile != null ? !indexFile.equals(that.indexFile) : that.indexFile != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fromInt;
        result = 31 * result + toInt;
        result = 31 * result + (indexFile != null ? indexFile.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "IntegerSnappyIndexFile{" +
                "fromInt=" + fromInt +
                ", toInt=" + toInt +
                ", indexFile=" + indexFile +
                '}';
    }
}