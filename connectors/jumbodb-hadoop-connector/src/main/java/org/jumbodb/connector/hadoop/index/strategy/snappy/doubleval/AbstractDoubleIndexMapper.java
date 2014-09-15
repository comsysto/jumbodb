package org.jumbodb.connector.hadoop.index.strategy.snappy.doubleval;

import org.apache.hadoop.io.DoubleWritable;
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
public abstract class AbstractDoubleIndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String DOUBLE_SNAPPY_V_1 = "DOUBLE_SNAPPY_V1";

    private DoubleWritable keyW = new DoubleWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        Double indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            keyW.set(indexableValue);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public String getStrategy() {
        return DOUBLE_SNAPPY_V_1;
    }


    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return DoubleRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return DoubleWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return DoubleSnappyIndexV1OutputFormat.class;
    }

    public abstract Double getIndexableValue(T input);
}
