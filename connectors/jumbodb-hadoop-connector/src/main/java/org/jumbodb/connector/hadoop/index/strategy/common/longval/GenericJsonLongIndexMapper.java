package org.jumbodb.connector.hadoop.index.strategy.common.longval;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public class GenericJsonLongIndexMapper extends AbstractLongIndexMapper<JsonNode> {

    private IndexField indexField;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
        if(indexField.getFields().size() != 1) {
            throw new RuntimeException("GenericJsonLongIndexMapper indexField must exactly contain one value!");
        }
    }

    @Override
    public Long getIndexableValue(JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if(!valueFor.isMissingNode()) {
            return valueFor.longValue();
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
