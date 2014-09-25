package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.connector.hadoop.index.strategy.common.datetime.GenericJsonDateTimeIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.doubleval.GenericJsonDoubleIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.floatval.GenericJsonFloatIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.geohash.GenericJsonGeohashIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode32.GenericJsonHashCode32IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.hashcode64.GenericJsonHashCode64IndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.integer.GenericJsonIntegerIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.common.longval.GenericJsonLongIndexMapper;
import org.jumbodb.connector.hadoop.index.strategy.lz4.*;
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
        indexMapper.put(HashCode32SnappyIndexOutputFormat.HASHCODE32_SNAPPY, new IndexStrategy(GenericJsonHashCode32IndexMapper.class, HashCode32SnappyIndexOutputFormat.class));
        indexMapper.put(HashCode64SnappyIndexOutputFormat.HASHCODE64_SNAPPY, new IndexStrategy(GenericJsonHashCode64IndexMapper.class, HashCode64SnappyIndexOutputFormat.class));
        indexMapper.put(IntegerSnappyIndexOutputFormat.INTEGER_SNAPPY, new IndexStrategy(GenericJsonIntegerIndexMapper.class, IntegerSnappyIndexOutputFormat.class));
        indexMapper.put(LongSnappyIndexOutputFormat.LONG_SNAPPY, new IndexStrategy(GenericJsonLongIndexMapper.class, LongSnappyIndexOutputFormat.class));
        indexMapper.put(FloatSnappyIndexOutputFormat.FLOAT_SNAPPY, new IndexStrategy(GenericJsonFloatIndexMapper.class, FloatSnappyIndexOutputFormat.class));
        indexMapper.put(DoubleSnappyIndexOutputFormat.DOUBLE_SNAPPY, new IndexStrategy(GenericJsonDoubleIndexMapper.class, DoubleSnappyIndexOutputFormat.class));
        indexMapper.put(GeohashSnappyIndexOutputFormat.GEOHASH_SNAPPY, new IndexStrategy(GenericJsonGeohashIndexMapper.class, GeohashSnappyIndexOutputFormat.class));
        indexMapper.put(DateTimeSnappyIndexOutputFormat.DATETIME_SNAPPY, new IndexStrategy(GenericJsonDateTimeIndexMapper.class, DateTimeSnappyIndexOutputFormat.class));

        indexMapper.put(HashCode32Lz4IndexOutputFormat.HASHCODE32_LZ4, new IndexStrategy(GenericJsonHashCode32IndexMapper.class, HashCode32Lz4IndexOutputFormat.class));
        indexMapper.put(HashCode64Lz4IndexOutputFormat.HASHCODE64_LZ4, new IndexStrategy(GenericJsonHashCode64IndexMapper.class, HashCode64Lz4IndexOutputFormat.class));
        indexMapper.put(IntegerLz4IndexOutputFormat.INTEGER_LZ4, new IndexStrategy(GenericJsonIntegerIndexMapper.class, IntegerLz4IndexOutputFormat.class));
        indexMapper.put(LongLz4IndexOutputFormat.LONG_LZ4, new IndexStrategy(GenericJsonLongIndexMapper.class, LongLz4IndexOutputFormat.class));
        indexMapper.put(FloatLz4IndexOutputFormat.FLOAT_LZ4, new IndexStrategy(GenericJsonFloatIndexMapper.class, FloatLz4IndexOutputFormat.class));
        indexMapper.put(DoubleLz4IndexOutputFormat.DOUBLE_LZ4, new IndexStrategy(GenericJsonDoubleIndexMapper.class, DoubleLz4IndexOutputFormat.class));
        indexMapper.put(GeohashLz4IndexOutputFormat.GEOHASH_LZ4, new IndexStrategy(GenericJsonGeohashIndexMapper.class, GeohashLz4IndexOutputFormat.class));
        indexMapper.put(DateTimeLz4IndexOutputFormat.DATETIME_LZ4, new IndexStrategy(GenericJsonDateTimeIndexMapper.class, DateTimeLz4IndexOutputFormat.class));
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
