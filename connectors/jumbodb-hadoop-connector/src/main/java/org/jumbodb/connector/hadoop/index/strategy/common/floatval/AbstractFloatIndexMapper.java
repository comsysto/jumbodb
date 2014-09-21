package org.jumbodb.connector.hadoop.index.strategy.common.floatval;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.snappy.FloatSnappyIndexOutputFormat;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractFloatIndexMapper<T> extends AbstractIndexMapper<T> {
    private FloatWritable keyW = new FloatWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        Float indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            keyW.set(indexableValue);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return FloatRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return FloatWritable.class;
    }

    public abstract Float getIndexableValue(T input);
}
