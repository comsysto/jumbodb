package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.geo.geohash.WGS84Point;
import org.jumbodb.common.query.QueryClause;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeoWithinRangeInMeterJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(QueryClause queryClause, Object value) {
        if(value instanceof List) {
            List<?> coords = (List<?>) queryClause.getValue();
            List<Number> point = (List<Number>) coords.get(0);
            double latitude = point.get(0).doubleValue();
            double longitude =  point.get(1).doubleValue();
            double distanceInMeter = ((Number)coords.get(1)).doubleValue();
            List<Number> valuePoint = (List<Number>) value;
            return GeoHash.distFromInMeter(latitude, longitude, valuePoint.get(0).doubleValue(), valuePoint.get(1).doubleValue()) < distanceInMeter;
        }
        return false;
    }
}
