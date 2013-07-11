package org.jumbodb.database.service.query;

import org.jumbodb.common.query.JsonQuery;

import java.util.List;

public class FileOffset implements Comparable<FileOffset> {
    private int fileNameHash;
    private long offset;
    private List<JsonQuery> jsonQueries;

    public FileOffset(int fileNameHash, long offset, List<JsonQuery> jsonQueries) {
        this.fileNameHash = fileNameHash;
        this.offset = offset;
        this.jsonQueries = jsonQueries;
    }

    public List<JsonQuery> getJsonQueries() {
        return jsonQueries;
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
        if (jsonQueries != null ? !jsonQueries.equals(that.jsonQueries) : that.jsonQueries != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = fileNameHash;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        result = 31 * result + (jsonQueries != null ? jsonQueries.hashCode() : 0);
        return result;
    }
}