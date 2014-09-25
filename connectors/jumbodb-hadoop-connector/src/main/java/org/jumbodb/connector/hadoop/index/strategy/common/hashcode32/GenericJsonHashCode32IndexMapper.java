package org.jumbodb.connector.hadoop.index.strategy.common.hashcode32;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 4:52 PM
 */
public class GenericJsonHashCode32IndexMapper extends AbstractHashCode32IndexMapper<JsonNode> {

    private IndexField indexField;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
    }

    @Override
    public String getIndexableValue(JsonNode input) {
        return getIndexKey(input);
    }

    @Override
    public String getIndexName() {
        return indexField.getIndexName();
    }

    @Override
    public Class<JsonNode> getJsonClass() {
        return JsonNode.class;
    }

    @Override
    public Class<? extends InputFormat> getPartitionerSamplingInputClass() {
        return HashCode32SamplingInputFormat.class;
    }

    private String getIndexKey(JsonNode jsonNode) {
        List<String> keys = new LinkedList<String>();
        for (String indexField : this.indexField.getFields()) {
            JsonNode valueFor = getNodeFor(indexField, jsonNode);
            if(!valueFor.isMissingNode()) {
                keys.add(valueFor.textValue());
            }
        }

        if(keys.size() > 0) {
            return StringUtils.join(keys, "-");
        }
        return null;
    }

    @Override
    public boolean throwErrorOnInvalidDataset() {
        return false;
    }
}
