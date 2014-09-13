package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.geo.geohash.WGS84Point;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeohashQueryValueRetriever implements QueryValueRetriever {

    private GeohashContainer value = new GeohashContainer();

    // four boxes where a geohash can start with a different value
    private static final BoundingBox upperLeft = new BoundingBox(new WGS84Point(0d, -180d), new WGS84Point(90d, -0.0000001d));
    private static final BoundingBox upperRight = new BoundingBox(new WGS84Point(0d, 0d), new WGS84Point(90d, 180d));
    private static final BoundingBox bottomLeft = new BoundingBox(new WGS84Point(-90d, -180d), new WGS84Point(-0.0000001d, -0.0000001d));
    private static final BoundingBox bottomRight = new BoundingBox(new WGS84Point(-90d, 0d), new WGS84Point(-0.0000001d, 180d));



    public GeohashQueryValueRetriever(IndexQuery indexQuery) {
        if(indexQuery.getQueryOperation() == QueryOperation.GEO_BOUNDARY_BOX) {
            List<List<Number>> coords = (List<List<Number>>) indexQuery.getValue();
            double latitude1 = coords.get(0).get(0).doubleValue();
            double longitude1 = coords.get(0).get(1).doubleValue();
            double latitude2 = coords.get(1).get(0).doubleValue();
            double longitude2 = coords.get(1).get(1).doubleValue();
            initializeBoundaryBox(latitude1, longitude1, latitude2, longitude2);
        }
        if(indexQuery.getQueryOperation() == QueryOperation.GEO_WITHIN_RANGE_METER) {
            // calculate boundary box from range ... later with geohashes it's anyway a square
            List<?> coords = (List<?>) indexQuery.getValue();
            List<Number> point = (List<Number>) coords.get(0);
            double latitude = point.get(0).doubleValue();
            double longitude =  point.get(1).doubleValue();
            double distanceInMeter = ((Number)coords.get(1)).doubleValue();
            double R = 6371; // earth radius in km
            double radius = distanceInMeter / 1000; // km
            double lon1 = longitude - Math.toDegrees(radius/R/Math.cos(Math.toRadians(latitude)));
            double lon2 = longitude + Math.toDegrees(radius/R/Math.cos(Math.toRadians(latitude)));
            double lat1 = latitude + Math.toDegrees(radius/R);
            double lat2 = latitude - Math.toDegrees(radius/R);
            value.setRangeMeterBox(new RangeMeterBox(latitude, longitude, distanceInMeter));
            initializeBoundaryBox(Math.min(lat1, lat2), Math.min(lon1, lon2), Math.max(lat1, lat2), Math.max(lon1, lon2));
        }
    }

    private void initializeBoundaryBox(double latitude1, double longitude1, double latitude2, double longitude2) {
        BoundingBox boundingBox = new BoundingBox(new WGS84Point(latitude1, longitude1), new WGS84Point(latitude2, longitude2));

        if(upperLeft.intersects(boundingBox)) {
            double minLat = upperLeft.getMinLat() > boundingBox.getMinLat() ? upperLeft.getMinLat() : boundingBox.getMinLat();
            double minLon = boundingBox.getMinLon();  // bleibt
            double maxLat = boundingBox.getMaxLat();  // bleibt
            double maxLon = upperLeft.getMaxLon() < boundingBox.getMaxLon() ? upperLeft.getMaxLon() : boundingBox.getMaxLon();
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), upperLeft));
        }
//        else {
//            value.add(createGeohashBoundaryBox(boundingBox, upperLeft));
//        }

        if(upperRight.intersects(boundingBox)) {
            double minLat = upperRight.getMinLat() > boundingBox.getMinLat() ? upperRight.getMinLat() : boundingBox.getMinLat();
            double minLon = upperRight.getMinLon() > boundingBox.getMinLon() ? upperRight.getMinLon() : boundingBox.getMinLon();
            double maxLat = boundingBox.getMaxLat();  // bleibt
            double maxLon = boundingBox.getMaxLon();  // bleibt
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), upperRight));
        }
//        else {
//            value.add(createGeohashBoundaryBox(boundingBox, upperRight));
//        }

        if(bottomLeft.intersects(boundingBox)) {
            double minLat = boundingBox.getMinLat();
            double minLon = boundingBox.getMinLon();
            double maxLat = bottomLeft.getMaxLat() < boundingBox.getMaxLat() ? bottomLeft.getMaxLat() : boundingBox.getMaxLat();
            double maxLon = bottomLeft.getMaxLon() < boundingBox.getMaxLon() ? bottomLeft.getMaxLon() : boundingBox.getMaxLon();
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), bottomLeft));
        }
//        else {
//            value.add(createGeohashBoundaryBox(boundingBox, bottomLeft));
//        }

        if(bottomRight.intersects(boundingBox)) {
            double minLat = boundingBox.getMinLat();  // bleibt
            double minLon = bottomRight.getMinLon() > boundingBox.getMinLon() ? bottomRight.getMinLon() : boundingBox.getMinLon();
            double maxLat = bottomRight.getMaxLat() < boundingBox.getMaxLat() ? bottomRight.getMaxLat() : boundingBox.getMaxLat();
            double maxLon = boundingBox.getMaxLon(); // bleibt
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), bottomRight));
        }
//        else {
//            value.add(createGeohashBoundaryBox(boundingBox, bottomRight));
//        }
    }

    private GeohashBoundaryBox createGeohashBoundaryBox(BoundingBox boundingBox, BoundingBox responseRange) {
        GeoHash geoHash1 = GeoHash.withBitPrecision(boundingBox.getMinLat(), boundingBox.getMinLon(), 32);
        GeoHash geoHash2 = GeoHash.withBitPrecision(boundingBox.getMaxLat(), boundingBox.getMaxLon(), 32);
        int geoHashInt1 = geoHash1.intValue();
        int geoHashInt2 = geoHash2.intValue();
        int bitsToShift = GeoHash.getBitsToShift(geoHashInt1, geoHashInt2);
        return new GeohashBoundaryBox(geoHashInt1 >> bitsToShift, bitsToShift, boundingBox, responseRange);
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
