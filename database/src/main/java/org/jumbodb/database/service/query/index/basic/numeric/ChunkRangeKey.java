package org.jumbodb.database.service.query.index.basic.numeric;

import java.io.File;

/**
 * @author Carsten Hufe
 */
public class ChunkRangeKey {
    private File file;
    private long chunkIndex;

    public ChunkRangeKey(File file, long chunkIndex) {
        this.file = file;
        this.chunkIndex = chunkIndex;
    }

    public File getFile() {
        return file;
    }

    public long getChunkIndex() {
        return chunkIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ChunkRangeKey that = (ChunkRangeKey) o;

        if (chunkIndex != that.chunkIndex) return false;
        if (file != null ? !file.equals(that.file) : that.file != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = file != null ? file.hashCode() : 0;
        result = 31 * result + (int) (chunkIndex ^ (chunkIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ChunkRangeKey{" +
                "file=" + file +
                ", chunkIndex=" + chunkIndex +
                '}';
    }
}
