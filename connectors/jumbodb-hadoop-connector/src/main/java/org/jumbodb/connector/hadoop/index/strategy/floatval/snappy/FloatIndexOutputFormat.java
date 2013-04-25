package org.jumbodb.connector.hadoop.index.strategy.floatval.snappy;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class FloatIndexOutputFormat extends AbstractIndexOutputFormat<FloatWritable, FileOffsetWritable> {

    @Override
    protected void write(FloatWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeFloat(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }
}
