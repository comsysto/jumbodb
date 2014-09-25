package org.jumbodb.connector.hadoop.index.strategy.common.doubleval;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.io.DoubleWritable;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

/**
 * Created by Carsten on 25.09.2014.
 */
public class DoubleSamplingInputFormat extends AbstractJsonFieldInputFormat<DoubleWritable> {
    private DoubleWritable doubleW = new DoubleWritable();

    @Override
    protected DoubleWritable getIndexableValue(IndexField indexField, JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if (!valueFor.isMissingNode()) {
            doubleW.set(valueFor.doubleValue());
            return doubleW;
        }
        return null;
    }
}
