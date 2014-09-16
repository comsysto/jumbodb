package org.jumbodb.connector.hadoop.data.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;

import java.io.IOException;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonIntegerSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    public static final String SORT_KEY = "INTEGER";

    private ObjectMapper jsonMapper;
    private IntWritable keyW = new IntWritable();
    private List<String> sortFields;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        sortFields = JumboConfigurationUtil.loadSortConfig(context.getConfiguration());
        if(sortFields.size() != 1) {
            throw new IllegalArgumentException("Sort fields must be exactly one!");
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JsonNode jsonNode = jsonMapper.readTree(value.toString());
        keyW.set(getSortKey(jsonNode));
        context.write(keyW, value);
    }

    private Integer getSortKey(JsonNode jsonNode) {
        return getValueFor(sortFields.get(0), jsonNode);
    }

    private Integer getValueFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            jsonNode = jsonNode.path(s);
        }
        if(jsonNode.isValueNode()) {
            Integer s = jsonNode.getIntValue();
            return s;
        }
        return 0;
    }
}
