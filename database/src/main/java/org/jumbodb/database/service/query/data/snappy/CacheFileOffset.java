package org.jumbodb.database.service.query.data.snappy;

import java.io.File;

/**
 * @author Carsten Hufe
 */
public class CacheFileOffset {
    private File file;
    private long offset;

    public CacheFileOffset(File file, long offset) {
        this.file = file;
        this.offset = offset;
    }

    public File getFile() {
        return file;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CacheFileOffset that = (CacheFileOffset) o;

        if (offset != that.offset) return false;
        if (file != null ? !file.equals(that.file) : that.file != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = file != null ? file.hashCode() : 0;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
