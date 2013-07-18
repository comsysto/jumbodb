package org.jumbodb.benchmark.suite.offsets;

import java.io.File;

/**
 * @author Carsten Hufe
 */
public class FileOffset {
    private File file;
    private long offset;

    public FileOffset(File file, long offset) {
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
    public String toString() {
        return "FileOffset{" +
                "file=" + file +
                ", offset=" + offset +
                '}';
    }
}
