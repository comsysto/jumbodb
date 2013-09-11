package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.geo.geohash.WGS84Point;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBoxQueryValueRetriever implements QueryValueRetriever {
    private GeohashBoundaryBoxContainer value = new GeohashBoundaryBoxContainer();

    // four boxes where a geohash can start with a different value
    private static final BoundingBox upperLeft = new BoundingBox(new WGS84Point(0d, -180d), new WGS84Point(90d, -0.0000001d));
    private static final BoundingBox upperRight = new BoundingBox(new WGS84Point(0d, 0d), new WGS84Point(90d, 180d));
    private static final BoundingBox bottomLeft = new BoundingBox(new WGS84Point(-90d, -180d), new WGS84Point(-0.0000001d, -0.0000001d));
    private static final BoundingBox bottomRight = new BoundingBox(new WGS84Point(-90d, 0d), new WGS84Point(-0.0000001d, 180d));

    public GeohashBoundaryBoxQueryValueRetriever(QueryClause queryClause) {
        List<List<Number>> coords = (List<List<Number>>) queryClause.getValue();
        double latitude1 = coords.get(0).get(0).doubleValue();
        double longitude1 = coords.get(0).get(1).doubleValue();
        double latitude2 = coords.get(1).get(0).doubleValue();
        double longitude2 = coords.get(1).get(1).doubleValue();
        BoundingBox boundingBox = new BoundingBox(new WGS84Point(latitude1, longitude1), new WGS84Point(latitude2, longitude2));

        if(upperLeft.intersects(boundingBox)) {
            double minLat = upperLeft.getMinLat() > boundingBox.getMinLat() ? upperLeft.getMinLat() : boundingBox.getMinLat();
            double minLon = boundingBox.getMinLon();  // bleibt
            double maxLat = boundingBox.getMaxLat();  // bleibt
            double maxLon = upperLeft.getMaxLon() < boundingBox.getMaxLon() ? upperLeft.getMaxLon() : boundingBox.getMaxLon();
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), upperLeft));
        }
        else {
            value.add(createGeohashBoundaryBox(boundingBox, upperLeft));
        }

        if(upperRight.intersects(boundingBox)) {
            double minLat = upperRight.getMinLat() > boundingBox.getMinLat() ? upperRight.getMinLat() : boundingBox.getMinLat();
            double minLon = upperRight.getMinLon() > boundingBox.getMinLon() ? upperRight.getMinLon() : boundingBox.getMinLon();
            double maxLat = boundingBox.getMaxLat();  // bleibt
            double maxLon = boundingBox.getMaxLon();  // bleibt
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), upperRight));
        }
        else {
            value.add(createGeohashBoundaryBox(boundingBox, upperRight));
        }

        if(bottomLeft.intersects(boundingBox)) {
            double minLat = boundingBox.getMinLat();
            double minLon = boundingBox.getMinLon();
            double maxLat = bottomLeft.getMaxLat() < boundingBox.getMaxLat() ? bottomLeft.getMaxLat() : boundingBox.getMaxLat();
            double maxLon = bottomLeft.getMaxLon() < boundingBox.getMaxLon() ? bottomLeft.getMaxLon() : boundingBox.getMaxLon();
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), bottomLeft));
        }
        else {
            value.add(createGeohashBoundaryBox(boundingBox, bottomLeft));
        }

        if(bottomRight.intersects(boundingBox)) {
            double minLat = boundingBox.getMinLat();  // bleibt
            double minLon = bottomRight.getMinLon() > boundingBox.getMinLon() ? bottomRight.getMinLon() : boundingBox.getMinLon();
            double maxLat = bottomRight.getMaxLat() < boundingBox.getMaxLat() ? bottomRight.getMaxLat() : boundingBox.getMaxLat();
            double maxLon = boundingBox.getMaxLon(); // bleibt
            value.add(createGeohashBoundaryBox(new BoundingBox(new WGS84Point(minLat, minLon), new WGS84Point(maxLat, maxLon)), bottomRight));
        }
        else {
            value.add(createGeohashBoundaryBox(boundingBox, bottomRight));
        }
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
