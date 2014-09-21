package org.jumbodb.connector.hadoop.index.strategy.lz4;

import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class IntegerLz4IndexOutputFormat extends AbstractLz4IndexOutputFormat<IntWritable, FileOffsetWritable> {
    public static final String INTEGER_LZ4 = "INTEGER_LZ4";

    @Override
    protected void write(IntWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeInt(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getLz4BlockSize() {
        return 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return INTEGER_LZ4;
    }
}
