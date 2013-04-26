package org.jumbodb.connector.hadoop.index.strategy.hashcode64.snappy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.common.query.HashCode64;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractHashCode64IndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String HASHCODE64_SNAPPY_V1 = "HASHCODE64_SNAPPY_V1";

    private LongWritable keyW = new LongWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        String indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            long hashCode = HashCode64.hash(indexableValue);
            keyW.set(hashCode);
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public String getStrategy() {
        return HASHCODE64_SNAPPY_V1;
    }


    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return HashCode64RangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return HashCode64IndexOutputFormat.class;
    }

    public abstract String getIndexableValue(T input);
}
