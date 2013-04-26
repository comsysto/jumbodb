package org.jumbodb.connector.hadoop.index.strategy.datetime.snappy;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.jumbodb.connector.hadoop.JumboConfigurationUtil;
import org.jumbodb.connector.hadoop.configuration.IndexField;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Carsten Hufe
 */
public class GenericJsonDateTimeIndexMapper extends AbstractDateTimeIndexMapper<JsonNode> {

    private IndexField indexField;
    private SimpleDateFormat sdf;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration configuration = context.getConfiguration();
        indexField = JumboConfigurationUtil.loadIndexJson(configuration);
        sdf = new SimpleDateFormat(indexField.getDatePattern());
        if(indexField.getFields().size() != 1) {
            throw new RuntimeException("GenericJsonDateTimeIndexMapper indexField must exactly contain one value!");
        }
    }

    @Override
    public Date getIndexableValue(JsonNode input) {
        JsonNode valueFor = getValueFor(indexField.getFields().get(0), input);
        if(valueFor != null) {
            String valueAsText = valueFor.getValueAsText();
            if(StringUtils.isNotBlank(valueAsText)) {
                try {
                    return sdf.parse(valueAsText);
                } catch (ParseException e) {
                    return new Date(0);
                }
            }
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
