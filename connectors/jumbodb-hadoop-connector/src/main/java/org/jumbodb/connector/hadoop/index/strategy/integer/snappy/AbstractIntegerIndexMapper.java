package org.jumbodb.connector.hadoop.index.strategy.integer.snappy;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractIntegerIndexMapper<T> extends AbstractIndexMapper<T> {
    public static final String INTEGER_SNAPPY_V_1 = "INTEGER_SNAPPY_V1";

    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        Integer indexableValue = getIndexableValue(input);
        if(indexableValue != null) {
            context.write(new IntWritable(indexableValue), new FileOffsetWritable(fileNameHashCode, offset.get()));
        }
    }

    @Override
    public String getStrategy() {
        return INTEGER_SNAPPY_V_1;
    }

    public abstract Integer getIndexableValue(T input);
}
