package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeohashWithingRangeMeterQueryValueRetriever implements QueryValueRetriever {
    private GeohashRangeMeterBox value;

    public GeohashWithingRangeMeterQueryValueRetriever(QueryClause queryClause) {
        List<?> coords = (List<?>) queryClause.getValue();
        List<Number> point = (List<Number>) coords.get(0);
        double latitude = point.get(0).doubleValue();
        double longitude =  point.get(1).doubleValue();
        double distanceInMeter = ((Number)coords.get(1)).doubleValue();
        GeoHash geoHash = GeoHash.withUnprecisionOfDistanceInMeter(latitude, longitude, distanceInMeter);
        int bitsToShift = 32 - geoHash.significantBits();
        value = new GeohashRangeMeterBox(geoHash.intValue() >> bitsToShift, bitsToShift, latitude, longitude, distanceInMeter);
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
