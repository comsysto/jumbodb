package org.jumbodb.connector.hadoop.index.strategy.integer.snappy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.codehaus.jackson.JsonNode;
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
        JsonNode valueFor = getValueFor(indexField.getFields().get(0), input);
        if(valueFor != null) {
            return valueFor.getIntValue();
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
