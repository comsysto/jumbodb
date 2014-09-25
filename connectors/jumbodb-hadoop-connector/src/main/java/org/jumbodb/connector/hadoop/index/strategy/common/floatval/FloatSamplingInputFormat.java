package org.jumbodb.connector.hadoop.index.strategy.common.floatval;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.io.FloatWritable;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

/**
 * Created by Carsten on 25.09.2014.
 */
public class FloatSamplingInputFormat extends AbstractJsonFieldInputFormat<FloatWritable> {
    private FloatWritable floatW = new FloatWritable();

    @Override
    protected FloatWritable getIndexableValue(IndexField indexField, JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if (!valueFor.isMissingNode()) {
            floatW.set(valueFor.floatValue());
            return floatW;
        }
        return null;
    }
}
