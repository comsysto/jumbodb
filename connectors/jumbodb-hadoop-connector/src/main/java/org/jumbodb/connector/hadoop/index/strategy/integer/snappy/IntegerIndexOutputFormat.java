package org.jumbodb.connector.hadoop.index.strategy.integer.snappy;

import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class IntegerIndexOutputFormat extends AbstractIndexOutputFormat<IntWritable, FileOffsetWritable> {

    @Override
    protected void write(IntWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeInt(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }
}
