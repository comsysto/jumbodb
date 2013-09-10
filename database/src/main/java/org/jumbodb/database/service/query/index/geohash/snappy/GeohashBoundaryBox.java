package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.WGS84Point;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBox {
    private int geohashFirstMatchingBits;
    private int bitsToShift;
    private BoundingBox boundingBox;
    private BoundingBox responsibleBox;

    public GeohashBoundaryBox(int geohashFirstMatchingBits, int bitsToShift, BoundingBox boundingBox, BoundingBox responsibleBox) {
        this.geohashFirstMatchingBits = geohashFirstMatchingBits;
        this.bitsToShift = bitsToShift;
        this.boundingBox = boundingBox;
        this.responsibleBox = responsibleBox;
    }

    public int getGeohashFirstMatchingBits() {
        return geohashFirstMatchingBits;
    }

    public int getBitsToShift() {
        return bitsToShift;
    }

    public double getLatitude1() {
        return boundingBox.getMinLat();
    }

    public double getLongitude1() {
        return boundingBox.getMinLon();
    }

    public double getLatitude2() {
        return boundingBox.getMaxLat();
    }

    public double getLongitude2() {
        return boundingBox.getMaxLon();
    }

    public boolean contains(double latitude, double longitude) {
        return boundingBox.contains(new WGS84Point(latitude, longitude));
    }

    public boolean isResponsibleFor(double latitude, double longitude) {
        return responsibleBox.contains(new WGS84Point(latitude, longitude));
    }

    @Override
    public String toString() {
        return "GeohashBoundaryBox{" +
                "geohashFirstMatchingBits=" + geohashFirstMatchingBits +
                ", bitsToShift=" + bitsToShift +
                ", boundingBox=" + boundingBox +
                ", responsibleBox=" + responsibleBox +
                '}';
    }
}
