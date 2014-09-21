package org.jumbodb.connector.hadoop.index.strategy.lz4;

import org.apache.hadoop.io.LongWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.DataOutputStream;
import java.io.IOException;

public class HashCode64Lz4IndexOutputFormat extends AbstractLz4IndexOutputFormat<LongWritable, FileOffsetWritable> {
    public static final String HASHCODE64_LZ4 = "HASHCODE64_LZ4";

    @Override
    protected void write(LongWritable k, FileOffsetWritable v,
                         DataOutputStream out) throws IOException, InterruptedException {
        out.writeLong(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getLz4BlockSize() {
        return 32 * 1020; // must be a multiple of 20! (8 byte long data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return HASHCODE64_LZ4;
    }
}
