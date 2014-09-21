package org.jumbodb.connector.hadoop.index.strategy.lz4;

import org.apache.hadoop.io.DoubleWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class DoubleLz4IndexOutputFormat extends AbstractLz4IndexOutputFormat<DoubleWritable, FileOffsetWritable> {
    public static final String DOUBLE_LZ4 = "DOUBLE_LZ4";

    @Override
    protected void write(DoubleWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeDouble(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getLz4BlockSize() {
        return 32 * 1020; // must be a multiple of 20! (8 byte double data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return DOUBLE_LZ4;
    }
}
