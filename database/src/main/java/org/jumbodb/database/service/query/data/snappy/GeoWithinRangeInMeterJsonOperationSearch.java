package org.jumbodb.database.service.query.data.snappy;

import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.query.JsonQuery;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeoWithinRangeInMeterJsonOperationSearch implements JsonOperationSearch {
    @Override
    public boolean matches(JsonQuery jsonQuery, Object value) {
        if (value instanceof List) {
            List<?> coords = (List<?>) jsonQuery.getValue();
            List<Number> point = (List<Number>) coords.get(0);
            double latitude = point.get(0).doubleValue();
            double longitude = point.get(1).doubleValue();
            double distanceInMeter = ((Number) coords.get(1)).doubleValue();
            List<Number> valuePoint = (List<Number>) value;
            double distance = GeoHash.distFromInMeter(latitude, longitude, valuePoint.get(0).doubleValue(), valuePoint.get(1).doubleValue());
            return distance < distanceInMeter;
        }
        return false;
    }
}
