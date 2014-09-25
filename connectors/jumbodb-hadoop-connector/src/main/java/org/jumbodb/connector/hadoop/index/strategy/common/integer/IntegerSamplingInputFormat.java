package org.jumbodb.connector.hadoop.index.strategy.common.integer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.io.IntWritable;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

/**
 * Created by Carsten on 25.09.2014.
 */
public class IntegerSamplingInputFormat extends AbstractJsonFieldInputFormat<IntWritable> {
    private IntWritable intW = new IntWritable();

    @Override
    protected IntWritable getIndexableValue(IndexField indexField, JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if(!valueFor.isMissingNode()) {
            intW.set(valueFor.intValue());
            return intW;
        }
        return null;
    }
}
