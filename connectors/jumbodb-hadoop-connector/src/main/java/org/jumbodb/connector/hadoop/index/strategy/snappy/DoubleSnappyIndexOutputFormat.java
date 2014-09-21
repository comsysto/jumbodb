package org.jumbodb.connector.hadoop.index.strategy.snappy;

import org.apache.hadoop.io.DoubleWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class DoubleSnappyIndexOutputFormat extends AbstractSnappyIndexOutputFormat<DoubleWritable, FileOffsetWritable> {
    public static final String DOUBLE_SNAPPY = "DOUBLE_SNAPPY";

    @Override
    protected void write(DoubleWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeDouble(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getSnappyBlockSize() {
        return 32 * 1020; // must be a multiple of 20! (8 byte double data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return DOUBLE_SNAPPY;
    }
}
