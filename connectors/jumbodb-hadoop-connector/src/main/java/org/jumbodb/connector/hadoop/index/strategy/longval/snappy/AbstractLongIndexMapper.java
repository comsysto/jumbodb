package org.jumbodb.connector.hadoop.index.strategy.longval.snappy;

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
public abstract class AbstractLongIndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String LONG_SNAPPY_V1 = "LONG_SNAPPY_V1";

    private LongWritable keyW = new LongWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        Long indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            keyW.set(indexableValue);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public String getStrategy() {
        return LONG_SNAPPY_V1;
    }


    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return LongRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return LongIndexOutputFormat.class;
    }

    public abstract Long getIndexableValue(T input);
}
