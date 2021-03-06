package org.jumbodb.connector.hadoop.index.strategy.integer.snappy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractIntegerIndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String INTEGER_SNAPPY_V1 = "INTEGER_SNAPPY_V1";
    private IntWritable keyW = new IntWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        Integer indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            keyW.set(indexableValue);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public String getStrategy() {
        return INTEGER_SNAPPY_V1;
    }


    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return IntegerRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return IntWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return IntegerSnappyIndexV1OutputFormat.class;
    }

    public abstract Integer getIndexableValue(T input);
}
