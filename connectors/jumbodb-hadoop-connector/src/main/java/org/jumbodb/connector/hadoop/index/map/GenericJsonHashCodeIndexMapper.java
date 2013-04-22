package org.jumbodb.connector.hadoop.index.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonHashCodeIndexMapper extends AbstractHashCodeIndexMapper<JsonNode> {
    public static final String JUMBO_INDEX_JSON_CONF = "jumbo.index.configuration";

    private IndexField indexField;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
    }

    @Override
    public String getIndexableValue(JsonNode input) {
        return getIndexKey(input);
    }

    @Override
    public String getIndexName() {
        return indexField.getIndexName();
    }

    @Override
    public Class<JsonNode> getJsonClass() {
        return JsonNode.class;
    }


    private String getIndexKey(JsonNode jsonNode) {
        List<String> keys = new LinkedList<String>();
        for (String indexField : this.indexField.getFields()) {
            JsonNode valueFor = getValueFor(indexField, jsonNode);
            if(valueFor != null) {
                keys.add(valueFor.getValueAsText());
            }
        }

        if(keys.size() > 0) {
            return StringUtils.join(keys, "-");
        }
        return null;
    }


    @Override
    public boolean throwErrorOnInvalidDataset() {
        return false;
    }
}
