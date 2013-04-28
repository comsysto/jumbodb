package org.jumbodb.connector.hadoop.index.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonDateTimeSortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public static final String SORT_KEY = "DATETIME";

    private ObjectMapper jsonMapper;
    private LongWritable keyW = new LongWritable();
    private List<String> sortFields;
    private SimpleDateFormat sdf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        sortFields = JumboConfigurationUtil.loadSortConfig(context.getConfiguration());
        String datePattern = JumboConfigurationUtil.loadSortDatePatternConfig(context.getConfiguration());
        sdf = new SimpleDateFormat(datePattern);
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
        String valueFor = getValueFor(sortFields.get(0), jsonNode);
        if(valueFor == null) {
            return 0l;
        }
        try {
            Date parse = sdf.parse(valueFor);
            return parse.getTime();
        } catch (ParseException e) {
            return 0l;
        }
    }

    private String getValueFor(String key, JsonNode jsonNode) {
        String[] split = StringUtils.split(key, ".");
        for (String s : split) {
            jsonNode = jsonNode.path(s);
        }
        if(jsonNode.isValueNode()) {
            String s = jsonNode.getTextValue();
            return s;
        }
        return null;
    }
}
