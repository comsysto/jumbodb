package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.index.strategy.common.datetime.GenericJsonDateTimeIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.doubleval.GenericJsonDoubleIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.floatval.GenericJsonFloatIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.geohash.GenericJsonGeohashIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode32.GenericJsonHashCode32IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode64.GenericJsonHashCode64IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.integer.GenericJsonIntegerIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.longval.GenericJsonLongIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.snappy.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class IndexStrategies {
    private static final Map<String, IndexStrategy> INDEX_STRATEGIES = createIndexMapper();

    private static Map<String, IndexStrategy> createIndexMapper() {
        Map<String, IndexStrategy> indexMapper = new HashMap<String, IndexStrategy>();
        indexMapper.put(HashCode32SnappyIndexOutputFormat.HASHCODE32_SNAPPY, new IndexStrategy(GenericJsonHashCode32IndexMapper.class, HashCode32SnappyIndexOutputFormat.class, 64));
        indexMapper.put(HashCode64SnappyIndexOutputFormat.HASHCODE64_SNAPPY, new IndexStrategy(GenericJsonHashCode64IndexMapper.class, HashCode64SnappyIndexOutputFormat.class, 64));
        indexMapper.put(IntegerSnappyIndexOutputFormat.INTEGER_SNAPPY, new IndexStrategy(GenericJsonIntegerIndexMapper.class, IntegerSnappyIndexOutputFormat.class, 64));
        indexMapper.put(LongSnappyIndexOutputFormat.LONG_SNAPPY, new IndexStrategy(GenericJsonLongIndexMapper.class, LongSnappyIndexOutputFormat.class, 64));
        indexMapper.put(FloatSnappyIndexOutputFormat.FLOAT_SNAPPY, new IndexStrategy(GenericJsonFloatIndexMapper.class, FloatSnappyIndexOutputFormat.class, 64));
        indexMapper.put(DoubleSnappyIndexOutputFormat.DOUBLE_SNAPPY, new IndexStrategy(GenericJsonDoubleIndexMapper.class, DoubleSnappyIndexOutputFormat.class, 64));
        indexMapper.put(GeohashSnappyIndexOutputFormat.GEOHASH_SNAPPY, new IndexStrategy(GenericJsonGeohashIndexMapper.class, GeohashSnappyIndexOutputFormat.class, 64));
        indexMapper.put(DateTimeSnappyIndexOutputFormat.DATETIME_SNAPPY, new IndexStrategy(GenericJsonDateTimeIndexMapper.class, DateTimeSnappyIndexOutputFormat.class, 64));
        return Collections.unmodifiableMap(indexMapper);
    }

    public static IndexStrategy getIndexStrategy(String strategyKey) {
        final IndexStrategy strategy = INDEX_STRATEGIES.get(strategyKey);
        if(strategy == null) {
            throw new IllegalStateException("Index strategy is not available " + strategyKey);
        }
        return strategy;
    }
}
