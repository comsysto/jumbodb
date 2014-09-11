package org.jumbodb.connector.hadoop.configuration;

import org.apache.hadoop.io.Text;
import org.jumbodb.connector.hadoop.index.output.data.SnappyDataV1InputFormat;
import org.jumbodb.connector.hadoop.index.output.data.SnappyDataV1OutputFormat;

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
        indexMapper.put(SnappyDataV1OutputFormat.STRATEGY_KEY, new JsonDataStrategy(SnappyDataV1InputFormat.class, SnappyDataV1OutputFormat.class,  Text.class));
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
