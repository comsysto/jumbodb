package org.jumbodb.connector.hadoop.index.strategy.lz4;

import org.apache.hadoop.io.FloatWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class FloatLz4IndexOutputFormat extends AbstractLz4IndexOutputFormat<FloatWritable, FileOffsetWritable> {
    public static final String FLOAT_LZ4 = "FLOAT_LZ4";

    @Override
    protected void write(FloatWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeFloat(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getLz4BlockSize() {
        return 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return FLOAT_LZ4;
    }
}
