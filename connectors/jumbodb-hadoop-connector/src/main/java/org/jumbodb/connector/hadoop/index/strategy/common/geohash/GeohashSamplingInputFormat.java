package org.jumbodb.connector.hadoop.index.strategy.common.geohash;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.connector.hadoop.configuration.IndexField;
import org.jumbodb.connector.hadoop.index.strategy.common.partition.AbstractJsonFieldInputFormat;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Carsten on 25.09.2014.
 */
public class GeohashSamplingInputFormat extends AbstractJsonFieldInputFormat<IntWritable> {
    private IntWritable intW = new IntWritable();

    @Override
    protected IntWritable getIndexableValue(IndexField indexFields, JsonNode input) {
        JsonNode valueFor = getNodeFor(indexFields.getFields().get(0), input);
        if(!valueFor.isArray()) {
            Double latitude = valueFor.get(0).asDouble();
            Double longitude = valueFor.get(1).asDouble();
            GeoHash geoHash = GeoHash.withBitPrecision(latitude, longitude, 32);
            intW.set(geoHash.intValue());
            return intW;
        }
        return null;
    }
}
