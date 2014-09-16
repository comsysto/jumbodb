package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.query.DataQuery;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeoWithinRangeInMeterDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(Object leftValue, Object rightValue) {
        // CARSTEN requires IN command with function, and function is always on the right
        if (leftValue instanceof List) {
            List<?> coords = (List<?>) rightValue;
            List<Number> point = (List<Number>) coords.get(0);
            double latitude = point.get(0).doubleValue();
            double longitude = point.get(1).doubleValue();
            double distanceInMeter = ((Number) coords.get(1)).doubleValue();
            List<Number> valuePoint = (List<Number>) leftValue;
            double distance = GeoHash.distFromInMeter(latitude, longitude, valuePoint.get(0).doubleValue(), valuePoint.get(1).doubleValue());
            return distance < distanceInMeter;
        }
        return false;
    }
}
