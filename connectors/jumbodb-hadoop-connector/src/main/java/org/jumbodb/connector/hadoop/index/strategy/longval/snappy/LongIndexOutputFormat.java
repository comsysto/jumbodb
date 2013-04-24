package org.jumbodb.connector.hadoop.index.strategy.longval.snappy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class LongIndexOutputFormat extends AbstractIndexOutputFormat<LongWritable> {

    @Override
    protected void write(LongWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeLong(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }
}
