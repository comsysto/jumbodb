package org.jumbodb.connector.hadoop.index.strategy.doubleval.snappy;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class DoubleIndexOutputFormat extends AbstractIndexOutputFormat<DoubleWritable, FileOffsetWritable> {

    @Override
    protected void write(DoubleWritable k, FileOffsetWritable v, DataOutputStream out) throws IOException, InterruptedException {
        out.writeDouble(k.get());
        out.writeInt(v.getFileNameHashCode());
        out.writeLong(v.getOffset());
    }
}
