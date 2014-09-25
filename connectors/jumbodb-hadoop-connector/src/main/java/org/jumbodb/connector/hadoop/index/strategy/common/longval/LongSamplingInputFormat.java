package org.jumbodb.connector.hadoop.index.strategy.common.longval;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.io.LongWritable;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

/**
 * Created by Carsten on 25.09.2014.
 */
public class LongSamplingInputFormat extends AbstractJsonFieldInputFormat<LongWritable> {
    private LongWritable longW = new LongWritable();

    @Override
    protected LongWritable getIndexableValue(IndexField indexField, JsonNode input) {
        JsonNode valueFor = getNodeFor(indexField.getFields().get(0), input);
        if (!valueFor.isMissingNode()) {
            longW.set(valueFor.longValue());
            return longW;
        }
        return null;
    }
}
