package org.jumbodb.connector.hadoop.index.strategy.floatval.snappy;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public class GenericJsonFloatIndexMapper extends AbstractFloatIndexMapper<JsonNode> {

    private IndexField indexField;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
        if(indexField.getFields().size() != 1) {
            throw new RuntimeException("GenericJsonFloatIndexMapper indexField must exactly contain one value!");
        }
    }

    @Override
    public Float getIndexableValue(JsonNode input) {
        JsonNode valueFor = getValueFor(indexField.getFields().get(0), input);
        if(valueFor != null) {
            return (float)valueFor.getValueAsDouble();
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
