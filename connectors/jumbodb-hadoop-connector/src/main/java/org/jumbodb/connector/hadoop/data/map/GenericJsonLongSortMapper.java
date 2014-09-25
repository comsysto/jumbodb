package org.jumbodb.connector.hadoop.data.map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;

import java.io.IOException;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonLongSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public static final String SORT_KEY = "LONG";

    private ObjectMapper jsonMapper;
    private LongWritable keyW = new LongWritable();
    private List<String> sortFields;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
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

    private Long getSortKey(JsonNode jsonNode) {
        return getValueFor(sortFields.get(0), jsonNode);
    }

    private Long getValueFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            jsonNode = jsonNode.path(s);
        }
        if(jsonNode.isValueNode()) {
            Long s = jsonNode.longValue();
            return s;
        }
        return 0l;
    }
}
