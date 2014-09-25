package org.jumbodb.connector.hadoop.index.strategy.common.integer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public class GenericJsonIntegerIndexMapper extends AbstractIntegerIndexMapper<JsonNode> {

    private IndexField indexField;

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
        if(indexField.getFields().size() != 1) {
            throw new RuntimeException("GenericJsonIntegerIndexMapper indexField must exactly contain one value!");
        }
    }

    @Override
    public Integer getIndexableValue(JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if(!valueFor.isMissingNode()) {
            return valueFor.intValue();
        }
        return null;
    }

    @Override
    public Class<? extends InputFormat> getPartitionerSamplingInputClass() {
        return IntegerSamplingInputFormat.class;
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
