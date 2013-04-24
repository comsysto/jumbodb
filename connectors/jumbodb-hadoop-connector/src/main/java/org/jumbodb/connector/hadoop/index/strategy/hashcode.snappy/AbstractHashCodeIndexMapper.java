package org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractHashCodeIndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String HASHCODE_SNAPPY_V_1 = "HASHCODE_SNAPPY_V1";

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        String indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            int hashCode = indexableValue.hashCode();
            context.write(new IntWritable(hashCode), new FileOffsetWritable(fileNameHashCode, offset.get()));
        }
    }

    @Override
    public String getStrategy() {
        return HASHCODE_SNAPPY_V_1;
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return HashRangePartitioner.class;
    }

    @Override
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return IntWritable.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormat() {
        return HashCodeIndexOutputFormat.class;
    }

    public abstract String getIndexableValue(T input);
}
