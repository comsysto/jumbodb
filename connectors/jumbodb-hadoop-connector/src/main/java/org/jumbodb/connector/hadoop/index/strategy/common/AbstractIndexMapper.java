package org.jumbodb.connector.hadoop.index.strategy.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);

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
            context.getCounter("json.index", "parse_exception").increment(1);
            System.err.println("Json " + ((FileSplit) context.getInputSplit()).getPath().toString());
            System.err.println("Json " + value.toString());
            if(throwErrorOnInvalidDataset()) {
                throw ex;
            }
        }
    }

    public JsonNode getNodeFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
//            if(jsonNode == null) {
//                break;
//            }
            jsonNode = jsonNode.path(s);
        }
        return jsonNode;
    }

    public List<String> getIndexSourceFields() {
        return Collections.emptyList();
    }

    public Class<?> getOutputValueClass() {
        return FileOffsetWritable.class;
    }

    public Class<? extends InputFormat> getPartitionerSamplingInputClass() {
        return null;
    }

    public abstract Class<? extends Partitioner> getPartitioner();
    public abstract Class<? extends WritableComparable> getOutputKeyClass();

    public abstract void onDataset(LongWritable offset, int fileNameHashCode, T input, Context context) throws IOException, InterruptedException;
    public abstract String getIndexName();
    public abstract Class<T> getJsonClass();

    public boolean throwErrorOnInvalidDataset() {
        return true;
    }
}
