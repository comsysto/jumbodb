package org.jumbodb.connector.hadoop.index.strategy.common.hashcode32;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.jumbodb.connector.hadoop.index.strategy.common.AbstractIndexMapper;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractHashCode32IndexMapper<T> extends AbstractIndexMapper<T> {

    private IntWritable keyW = new IntWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        String indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            int hashCode = indexableValue.hashCode();
            keyW.set(hashCode);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return HashCode32RangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return IntWritable.class;
    }

    public abstract String getIndexableValue(T input);
}
