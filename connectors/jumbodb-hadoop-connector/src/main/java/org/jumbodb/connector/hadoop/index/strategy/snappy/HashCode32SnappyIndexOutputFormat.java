package org.jumbodb.connector.hadoop.index.strategy.snappy;

import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class HashCode32SnappyIndexOutputFormat extends AbstractSnappyIndexOutputFormat<IntWritable, FileOffsetWritable> {
    public static final String HASHCODE32_SNAPPY = "HASHCODE32_SNAPPY";

    @Override
    protected void write(IntWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeInt(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getSnappyBlockSize() {
        return 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return HASHCODE32_SNAPPY;
    }
}
