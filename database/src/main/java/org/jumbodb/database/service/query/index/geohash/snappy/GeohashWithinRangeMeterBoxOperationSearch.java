package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.GeoHash;

/**
 * @author Carsten Hufe
 */
public class GeohashWithinRangeMeterBoxOperationSearch extends GeohashBoundaryBoxOperationSearch {

    @Override
    protected boolean containsPoint(GeohashCoords currentValue, GeohashBoundaryBox searchValue, GeohashContainer container) {
        RangeMeterBox rangeMeterBox = container.getRangeMeterBox();
        double distance = GeoHash.distFromInMeter(rangeMeterBox.getLatitude(), rangeMeterBox.getLongitude(), currentValue.getLatitude(), currentValue.getLongitude());
        return distance < rangeMeterBox.getRangeInMeter();
    }
}
