package org.jumbodb.connector.hadoop.index.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.index.data.FileOffsetWritable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * User: carsten
 * Date: 11/3/12
 * Time: 3:26 PM
 */
public abstract class AbstractIndexMapper<T> extends Mapper<LongWritable, Text, WritableComparable, FileOffsetWritable> {
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

        try {
            T input = jsonMapper.readValue(value.toString(), getJsonClass());
            onDataset(key, name.hashCode(), input, context);
        }
        catch(JsonParseException ex) {
            System.err.println("Json " + ((FileSplit) context.getInputSplit()).getPath().toString());
            System.err.println("Json " + value.toString());
            if(throwErrorOnInvalidDataset()) {
                throw ex;
            }
        }
    }

    public JsonNode getValueFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            if(jsonNode == null) {
                break;
            }
            jsonNode = jsonNode.path(s);
        }
        return jsonNode;

//        if(jsonNode.isValueNode()) {
//            return jsonNode;
//            String s = jsonNode.getValueAsText();
//            return s;
//        }
//        return null;
    }

    public List<String> getIndexSourceFields() {
        return Collections.emptyList();
    }

    public Class<?> getOutputValueClass() {
        return FileOffsetWritable.class;
    }

    public abstract Class<? extends Partitioner> getPartitioner();
    public abstract Class<? extends WritableComparable> getOutputKeyClass();
    public abstract Class<? extends OutputFormat> getOutputFormat();

    public abstract void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException;
    public abstract String getIndexName();
    public abstract String getStrategy();
    public abstract Class<T> getJsonClass();

    public boolean throwErrorOnInvalidDataset() {
        return false;
    }
}
