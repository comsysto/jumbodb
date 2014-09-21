package org.jumbodb.connector.hadoop.index.strategy.common.datetime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.snappy.DateTimeSnappyIndexOutputFormat;

import java.io.IOException;
import java.util.Date;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractDateTimeIndexMapper<T> extends AbstractIndexMapper<T> {
    private LongWritable keyW = new LongWritable();
    private FileOffsetWritable valueW = new FileOffsetWritable();

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        Date indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            keyW.set(indexableValue.getTime());
            valueW.setFileNameHashCode(fileNameHashCode);
            valueW.setOffset(offset.get());
            context.write(keyW, valueW);
        }
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return DateTimeRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return LongWritable.class;
    }

    public abstract Date getIndexableValue(T input);
}
