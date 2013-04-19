package org.jumbodb.connector.hadoop.index.map;

import org.jumbodb.connector.hadoop.index.IndexJobCreator;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractHashCodeIndexMapper<T> extends AbstractIndexMapper<T> {
    @Override
    public void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException {
        String indexableValue = getIndexableValue(input);
        int hashCode = 0;
        if(indexableValue != null) {
            hashCode = indexableValue.hashCode();
        }
        context.write(new IntWritable(hashCode), new FileOffsetWritable(fileNameHashCode, offset.get()));
    }

    @Override
    public String getStrategy() {
        return IndexJobCreator.HASHCODE_SNAPPY_V_1;
    }

    public abstract String getIndexableValue(T input);
}
