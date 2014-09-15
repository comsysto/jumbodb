package org.jumbodb.database.service.query.index.common.numeric;

import java.io.File;

public class NumberIndexFile<T> {
    private T from;
    private T to;
    private File indexFile;

    public NumberIndexFile(T from, T to, File indexFile) {
        this.from = from;
        this.to = to;
        this.indexFile = indexFile;
    }


    public T getFrom() {
        return from;
    }

    public T getTo() {
        return to;
    }

    public File getIndexFile() {
        return indexFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NumberIndexFile that = (NumberIndexFile) o;

        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        if (indexFile != null ? !indexFile.equals(that.indexFile) : that.indexFile != null) return false;
        if (to != null ? !to.equals(that.to) : that.to != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (indexFile != null ? indexFile.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NumberSnappyIndexFile{" +
                "from=" + from +
                ", to=" + to +
                ", indexFile=" + indexFile +
                '}';
    }
}