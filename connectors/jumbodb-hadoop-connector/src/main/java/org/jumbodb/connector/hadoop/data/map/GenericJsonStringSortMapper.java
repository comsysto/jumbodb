package org.jumbodb.connector.hadoop.data.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonStringSortMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static final String SORT_KEY = "STRING";

    private ObjectMapper jsonMapper;
    private Text keyW = new Text();
    private List<String> sortFields;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        sortFields = JumboConfigurationUtil.loadSortConfig(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JsonNode jsonNode = jsonMapper.readTree(value.toString());
            keyW.set(getSortKey(jsonNode));
            context.write(keyW, value);
        } catch(JsonParseException e) {
            throw new RuntimeException(context.getInputSplit() + " +++ " + value.toString(), e);
        }
    }

    private String getSortKey(JsonNode jsonNode) {
        List<String> keys = new LinkedList<String>();
        for (String sort : sortFields) {
            String valueFor = getValueFor(sort, jsonNode);
            if(valueFor != null) {
                keys.add(valueFor);
            }
        }

        if(keys.size() > 0) {
            return StringUtils.join(keys, "-");
        }
        return "default";
    }

    private String getValueFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            jsonNode = jsonNode.path(s);
        }
        if(jsonNode.isValueNode()) {
            String s = jsonNode.getValueAsText();
            return s;
        }
        return "null";
    }
}
