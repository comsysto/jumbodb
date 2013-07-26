package org.jumbodb.benchmark.suite.data.strategy;

import org.jumbodb.benchmark.suite.data.strategy.json.plain.JsonPlainDataStrategy;

/**
 * @author Carsten Hufe
 */
public class DataStrategies {
    public static Class<? extends DataStrategy> getStrategy(String strategy) {
        if(JsonPlainDataStrategy.STRATEGY_NAME.contentEquals(strategy)) {
            return JsonPlainDataStrategy.class;
        }
        throw new IllegalArgumentException(strategy + " is not supported");
    }
}
