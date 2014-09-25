package org.jumbodb.connector.hadoop.index.strategy.common.hashcode64;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.jumbodb.common.query.HashCode64;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 25.09.2014.
 */
public class HashCode64SamplingInputFormat extends AbstractJsonFieldInputFormat<LongWritable> {
    private LongWritable longW = new LongWritable();

    @Override
    protected LongWritable getIndexableValue(IndexField indexFields, JsonNode input) {
        List<String> keys = new LinkedList<String>();
        for (String indexField : indexFields.getFields()) {
            JsonNode valueFor = getNodeFor(indexField, input);
            if(!valueFor.isMissingNode()) {
                keys.add(valueFor.textValue());
            }
        }

        if(keys.size() > 0) {
            String join = StringUtils.join(keys, "-");
            longW.set(HashCode64.hash(join));
            return longW;
        }
        return null;
    }
}
