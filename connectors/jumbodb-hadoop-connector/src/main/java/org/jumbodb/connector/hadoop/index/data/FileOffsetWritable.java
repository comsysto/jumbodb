package org.jumbodb.connector.hadoop.index.data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: carsten
 * Date: 11/22/12
 * Time: 5:12 PM
 */
public class FileOffsetWritable implements Writable {
    private int fileNameHashCode;
    private long offset;

    public FileOffsetWritable() {
    }

    public FileOffsetWritable(int fileNameHashCode, long offset) {
        this.fileNameHashCode = fileNameHashCode;
        this.offset = offset;
    }

    public int getFileNameHashCode() {
        return fileNameHashCode;
    }

    public void setFileNameHashCode(int fileNameHashCode) {
        this.fileNameHashCode = fileNameHashCode;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(fileNameHashCode);
        dataOutput.writeLong(offset);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileNameHashCode = dataInput.readInt();
        offset = dataInput.readLong();
    }
}
