package org.jumbodb.importer.hadoop.json.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.connector.hadoop.HadoopConfigurationUtil;
import org.jumbodb.importer.hadoop.json.JsonImporterJob;
import org.jumbodb.connector.hadoop.index.json.ImportJson;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonSortMapper extends Mapper<LongWritable, Text, Text, Text> {
    private ObjectMapper jsonMapper;
    private Text keyW = new Text();
    private ImportJson importJson;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        jsonMapper = new ObjectMapper();
        jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        importJson = HadoopConfigurationUtil.loadImportJson(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JsonNode jsonNode = jsonMapper.readTree(value.toString());
        keyW.set(getSortKey(jsonNode));
        context.write(keyW, value);
    }

    private String getSortKey(JsonNode jsonNode) {
        List<String> keys = new LinkedList<String>();
        for (String sort : importJson.getSort()) {
            keys.add(getValueFor(sort,  jsonNode));
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
            // CARSTEN ist das richtig?
            String s = jsonNode.asText();
            return s;
        }
        throw new RuntimeException("index key references on container node: " + key);
    }
}
