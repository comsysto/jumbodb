package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.io.Text;
import org.jumbodb.connector.hadoop.data.output.lz4.JsonLz4DataInputFormat;
import org.jumbodb.connector.hadoop.data.output.lz4.JsonLz4DataOutputFormat;
import org.jumbodb.connector.hadoop.data.output.snappy.JsonSnappyDataInputFormat;
import org.jumbodb.connector.hadoop.data.output.snappy.JsonSnappyDataOutputFormat;
import org.jumbodb.connector.hadoop.data.output.snappy.JsonSnappyLineBreakDataInputFormat;
import org.jumbodb.connector.hadoop.data.output.snappy.JsonSnappyLineBreakDataOutputFormat;

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
                new JsonDataStrategy(JsonSnappyLineBreakDataInputFormat.class, JsonSnappyLineBreakDataOutputFormat.class, Text.class)
        );
        indexMapper.put(
                JsonSnappyDataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(JsonSnappyDataInputFormat.class, JsonSnappyDataOutputFormat.class, Text.class)
        );
        indexMapper.put(
                JsonLz4DataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(JsonLz4DataInputFormat.class, JsonLz4DataOutputFormat.class, Text.class)
        );
        return Collections.unmodifiableMap(indexMapper);
    }

    public static DataStrategy getDataStrategy(String strategyKey) {
        final DataStrategy strategy = DATA_STRATEGIES.get(strategyKey);
        if (strategy == null) {
            throw new IllegalStateException("Data strategy is not available " + strategyKey);
        }
        return strategy;
    }
}
