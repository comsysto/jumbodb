package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.io.Text;
import org.jumbodb.connector.hadoop.data.output.JsonSnappyDataInputFormat;
import org.jumbodb.connector.hadoop.data.output.JsonSnappyDataOutputFormat;
import org.jumbodb.connector.hadoop.data.output.JsonSnappyLineBreakDataInputFormat;
import org.jumbodb.connector.hadoop.data.output.JsonSnappyLineBreakDataOutputFormat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class DataStrategies {
    private static final Map<String, ? extends DataStrategy> DATA_STRATEGIES = createDataStrategies();

    private static Map<String, ? extends DataStrategy> createDataStrategies() {
        Map<String, DataStrategy> indexMapper = new HashMap<String, DataStrategy>();
        indexMapper.put(
          JsonSnappyLineBreakDataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(JsonSnappyLineBreakDataInputFormat.class, JsonSnappyLineBreakDataOutputFormat.class,  Text.class)
        );
        indexMapper.put(
                JsonSnappyDataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(JsonSnappyDataInputFormat.class, JsonSnappyDataOutputFormat.class,  Text.class)
        );
        return Collections.unmodifiableMap(indexMapper);
    }

    public static DataStrategy getDataStrategy(String strategyKey) {
        final DataStrategy strategy = DATA_STRATEGIES.get(strategyKey);
        if(strategy == null) {
            throw new IllegalStateException("Data strategy is not available " + strategyKey);
        }
        return strategy;
    }
}
