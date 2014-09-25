package org.jumbodb.connector.hadoop.data.map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonGeohashSortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    public static final String SORT_KEY = "GEOHASH";

    private ObjectMapper jsonMapper;
    private IntWritable keyW = new IntWritable();
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

    private Integer getSortKey(JsonNode jsonNode) {
        List<Double> valueFor = getValueFor(sortFields.get(0), jsonNode);
        if(valueFor != null) {
            return GeoHash.withBitPrecision(valueFor.get(0), valueFor.get(1), 32).intValue();
        }
        return 0;
    }

    private List<Double> getValueFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            jsonNode = jsonNode.path(s);
        }
        if(jsonNode.isArray()) {
            List<Double> result = new ArrayList<Double>(2);
            for (JsonNode node : jsonNode) {
                result.add(node.doubleValue());
            }
            return result;
        }
        return null;
    }
}
