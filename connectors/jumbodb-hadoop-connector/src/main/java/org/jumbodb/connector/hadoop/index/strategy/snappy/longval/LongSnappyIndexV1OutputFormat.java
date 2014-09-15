package org.jumbodb.connector.hadoop.index.strategy.snappy.longval;

import org.apache.hadoop.io.LongWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.index.AbstractSnappyIndexV1OutputFormat;
import org.jumbodb.connector.hadoop.index.strategy.snappy.hashcode64.AbstractHashCode64IndexMapper;

import java.io.DataOutputStream;
import java.io.IOException;

public class LongSnappyIndexV1OutputFormat extends AbstractSnappyIndexV1OutputFormat<LongWritable, FileOffsetWritable> {

    @Override
    protected void write(LongWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeLong(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getSnappyBlockSize() {
        return 32 * 1020; // must be a multiple of 20! (8 byte long data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return AbstractHashCode64IndexMapper.HASHCODE64_SNAPPY_V1;
    }
}
