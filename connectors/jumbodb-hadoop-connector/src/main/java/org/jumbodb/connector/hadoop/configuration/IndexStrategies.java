package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.index.output.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.datetime.GenericJsonDateTimeIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.doubleval.GenericJsonDoubleIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.floatval.GenericJsonFloatIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.geohash.GenericJsonGeohashIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode32.GenericJsonHashCode32IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode64.GenericJsonHashCode64IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.integer.GenericJsonIntegerIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.longval.GenericJsonLongIndexMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class IndexStrategies {
    private static final Map<String, Class<? extends AbstractIndexMapper>> INDEX_STRATEGIES = createIndexMapper();

    private static Map<String, Class<? extends AbstractIndexMapper>> createIndexMapper() {
        Map<String, Class<? extends AbstractIndexMapper>> indexMapper = new HashMap<String, Class<? extends AbstractIndexMapper>>();
        indexMapper.put(GenericJsonHashCode32IndexMapper.HASHCODE32_SNAPPY, GenericJsonHashCode32IndexMapper.class);
        indexMapper.put(GenericJsonHashCode64IndexMapper.HASHCODE64_SNAPPY, GenericJsonHashCode64IndexMapper.class);
        indexMapper.put(GenericJsonIntegerIndexMapper.INTEGER_SNAPPY, GenericJsonIntegerIndexMapper.class);
        indexMapper.put(GenericJsonLongIndexMapper.LONG_SNAPPY, GenericJsonLongIndexMapper.class);
        indexMapper.put(GenericJsonFloatIndexMapper.FLOAT_SNAPPY, GenericJsonFloatIndexMapper.class);
        indexMapper.put(GenericJsonDoubleIndexMapper.DOUBLE_SNAPPY, GenericJsonDoubleIndexMapper.class);
        indexMapper.put(GenericJsonGeohashIndexMapper.GEOHASH_SNAPPY, GenericJsonGeohashIndexMapper.class);
        indexMapper.put(GenericJsonDateTimeIndexMapper.DATETIME_SNAPPY, GenericJsonDateTimeIndexMapper.class);
        return Collections.unmodifiableMap(indexMapper);
    }

    public static Class<? extends AbstractIndexMapper> getIndexStrategy(String strategyKey) {
        final Class<? extends AbstractIndexMapper> strategy = INDEX_STRATEGIES.get(strategyKey);
        if(strategy == null) {
            throw new IllegalStateException("Index strategy is not available " + strategyKey);
        }
        return strategy;
    }
}
