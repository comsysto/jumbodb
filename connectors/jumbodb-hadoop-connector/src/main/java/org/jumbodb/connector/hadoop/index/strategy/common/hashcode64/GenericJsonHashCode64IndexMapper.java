package org.jumbodb.connector.hadoop.index.strategy.common.hashcode64;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GenericJsonHashCode64IndexMapper extends AbstractHashCode64IndexMapper<JsonNode> {

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

    private String getIndexKey(JsonNode jsonNode) {
        List<String> keys = new LinkedList<String>();
        for (String indexField : this.indexField.getFields()) {
            JsonNode valueFor = getNodeFor(indexField, jsonNode);
            if(!valueFor.isMissingNode()) {
                keys.add(valueFor.textValue());
            }
        }

        if(keys.size() > 0) {
            return StringUtils.join(keys, "-");
        }
        return null;
    }

    @Override
    public String getIndexName() {
        return indexField.getIndexName();
    }

    @Override
    public Class<JsonNode> getJsonClass() {
        return JsonNode.class;
    }
}
