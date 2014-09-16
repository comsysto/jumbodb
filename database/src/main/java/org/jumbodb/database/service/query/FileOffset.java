package org.jumbodb.database.service.query;

import org.jumbodb.common.query.IndexQuery;

public class FileOffset implements Comparable<FileOffset> {
    private int fileNameHash;
    private long offset;
    private IndexQuery indexQuery;

    public FileOffset(int fileNameHash, long offset, IndexQuery indexQuery) {
        this.fileNameHash = fileNameHash;
        this.offset = offset;
        this.indexQuery = indexQuery;
    }

    public IndexQuery getIndexQuery() {
        return indexQuery;
    }

    public int getFileNameHash() {
        return fileNameHash;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public int compareTo(FileOffset fileOffset) {
        long l1 = offset;
        long l2 = fileOffset.offset;
        return l1 == l2 ? 0 : l1 < l2 ? -1 : 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileOffset that = (FileOffset) o;

        if (fileNameHash != that.fileNameHash) return false;
        if (offset != that.offset) return false;
        if (indexQuery != null ? !indexQuery.equals(that.indexQuery) : that.indexQuery != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fileNameHash;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (indexQuery != null ? indexQuery.hashCode() : 0);
        return result;
    }
}