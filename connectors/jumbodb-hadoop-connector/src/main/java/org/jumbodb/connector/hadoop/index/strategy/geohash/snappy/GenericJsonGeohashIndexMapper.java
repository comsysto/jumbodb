package org.jumbodb.connector.hadoop.index.strategy.geohash.snappy;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GenericJsonGeohashIndexMapper extends AbstractGeohashIndexMapper<JsonNode> {

    private IndexField indexField;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
        if(indexField.getFields().size() != 1) {
            throw new RuntimeException("GenericJsonIntegerIndexMapper indexField must exactly contain one value!");
        }
    }

    @Override
    public List<Double> getIndexableValue(JsonNode input) {
        JsonNode valueFor = getValueFor(indexField.getFields().get(0), input);
        if(valueFor != null) {
//            Iterator<JsonNode> elements = valueFor.getElements();
            List<Double> result = new ArrayList<Double>(2);
            for (JsonNode jsonNode : valueFor) {
                result.add(jsonNode.getValueAsDouble());
            }
            return result;
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
