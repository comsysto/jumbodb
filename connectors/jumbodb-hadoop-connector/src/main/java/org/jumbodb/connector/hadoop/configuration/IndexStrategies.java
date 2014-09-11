package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.index.map.AbstractIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.datetime.snappy.GenericJsonDateTimeIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.doubleval.snappy.GenericJsonDoubleIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.floatval.snappy.GenericJsonFloatIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.geohash.snappy.GenericJsonGeohashIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.hashcode.snappy.GenericJsonHashCode32IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.hashcode64.snappy.GenericJsonHashCode64IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.integer.snappy.GenericJsonIntegerIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.longval.snappy.GenericJsonLongIndexMapper;

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
        indexMapper.put(GenericJsonHashCode32IndexMapper.HASHCODE32_SNAPPY_V1, GenericJsonHashCode32IndexMapper.class);
        indexMapper.put(GenericJsonHashCode64IndexMapper.HASHCODE64_SNAPPY_V1, GenericJsonHashCode64IndexMapper.class);
        indexMapper.put(GenericJsonIntegerIndexMapper.INTEGER_SNAPPY_V1, GenericJsonIntegerIndexMapper.class);
        indexMapper.put(GenericJsonLongIndexMapper.LONG_SNAPPY_V1, GenericJsonLongIndexMapper.class);
        indexMapper.put(GenericJsonFloatIndexMapper.FLOAT_SNAPPY_V_1, GenericJsonFloatIndexMapper.class);
        indexMapper.put(GenericJsonDoubleIndexMapper.DOUBLE_SNAPPY_V_1, GenericJsonDoubleIndexMapper.class);
        indexMapper.put(GenericJsonGeohashIndexMapper.GEOHASH_SNAPPY_V1, GenericJsonGeohashIndexMapper.class);
        indexMapper.put(GenericJsonDateTimeIndexMapper.DATETIME_SNAPPY_V1, GenericJsonDateTimeIndexMapper.class);
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
