package org.jumbodb.connector.hadoop.index.strategy.snappy.datetime;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;

import java.io.IOException;
import java.util.Date;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractDateTimeIndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String DATETIME_SNAPPY = "DATETIME_SNAPPY";

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
    public String getStrategy() {
        // CARSTEN remove version from name
        return DATETIME_SNAPPY;
    }


    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return DateTimeRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return LongWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return DateTimeSnappyIndexOutputFormat.class;
    }

    public abstract Date getIndexableValue(T input);
}
