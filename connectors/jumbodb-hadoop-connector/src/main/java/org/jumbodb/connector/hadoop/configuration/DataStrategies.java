package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.io.Text;
import org.jumbodb.connector.hadoop.data.output.lz4.*;
import org.jumbodb.connector.hadoop.data.output.snappy.*;

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
                MsgPackSnappyDataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(MsgPackSnappyDataInputFormat.class, MsgPackSnappyDataOutputFormat.class, Text.class)
        );
        indexMapper.put(
                JsonLz4DataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(JsonLz4DataInputFormat.class, JsonLz4DataOutputFormat.class, Text.class)
        );
        indexMapper.put(
                JsonLz4LineBreakDataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(JsonLz4LineBreakDataInputFormat.class, JsonLz4LineBreakDataOutputFormat.class, Text.class)
        );
        indexMapper.put(
                MsgPackLz4DataOutputFormat.STRATEGY_KEY,
                new JsonDataStrategy(MsgPackLz4DataInputFormat.class, MsgPackLz4DataOutputFormat.class, Text.class)
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
