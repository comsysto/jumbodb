package org.jumbodb.connector.hadoop.index.strategy.snappy.floatval;

import org.apache.hadoop.io.FloatWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractSnappyIndexOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class FloatSnappyIndexOutputFormat extends AbstractSnappyIndexOutputFormat<FloatWritable, FileOffsetWritable> {

    @Override
    protected void write(FloatWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeFloat(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }

    @Override
    protected int getSnappyBlockSize() {
        return 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)
    }

    @Override
    protected String getStrategy() {
        return AbstractFloatIndexMapper.FLOAT_SNAPPY;
    }
}
