package org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy;

import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.index.AbstractSnappyIndexV1OutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class HashCode32SnappyIndexV1OutputFormat extends AbstractSnappyIndexV1OutputFormat<IntWritable, FileOffsetWritable> {

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
        return AbstractHashCode32IndexMapper.HASHCODE32_SNAPPY_V1;
    }
}
