package org.jumbodb.connector.hadoop.index.map;

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
public abstract class AbstractIndexMapper<T> extends Mapper<LongWritable, Text, IntWritable, FileOffsetWritable> {
    private ObjectMapper jsonMapper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    }

    @Override
    protected final void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit sp = (FileSplit)context.getInputSplit();
        String name = sp.getPath().getName();

        T input = jsonMapper.readValue(value.toString(), getJsonClass());
        String indexableValue = getIndexableValue(input);
        context.write(new IntWritable(indexableValue.hashCode()), new FileOffsetWritable(name.hashCode(), key.get()));
    }

    public abstract String getIndexableValue(T input);
    public abstract String getIndexName();
    public abstract Class<T> getJsonClass();
}
