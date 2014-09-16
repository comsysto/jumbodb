package org.jumbodb.database.service.query.data.common;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.WGS84Point;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeoBoundaryBoxDataOperationSearch implements DataOperationSearch {
    @Override
    public boolean matches(Object leftValue, Object rightValue) {
        if (leftValue instanceof List) {
            List<List<Number>> coords = (List<List<Number>>) rightValue;
            List<Number> valuePoint = (List<Number>) leftValue;
            double latitude1 = coords.get(0).get(0).doubleValue();
            double longitude1 = coords.get(0).get(1).doubleValue();
            double latitude2 = coords.get(1).get(0).doubleValue();
            double longitude2 = coords.get(1).get(1).doubleValue();
            BoundingBox boundingBox = new BoundingBox(new WGS84Point(latitude1, longitude1),
              new WGS84Point(latitude2, longitude2));
            return boundingBox
              .contains(new WGS84Point(valuePoint.get(0).doubleValue(), valuePoint.get(1).doubleValue()));
        }
        return false;
    }
}
