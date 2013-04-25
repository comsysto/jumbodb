package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.WGS84Point;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBox {
//    private int geohash;
    private int geohashFirstMatchingBits;
    private int bitsToShift;
    private double latitude1;
    private double longitude1;
    private double latitude2;
    private double longitude2;
    private BoundingBox boundingBox;

    public GeohashBoundaryBox(int geohashFirstMatchingBits, int bitsToShift, double latitude1, double longitude1, double latitude2, double longitude2) {
        this.geohashFirstMatchingBits = geohashFirstMatchingBits;
        this.bitsToShift = bitsToShift;
        this.latitude1 = latitude1;
        this.longitude1 = longitude1;
        this.latitude2 = latitude2;
        this.longitude2 = longitude2;
        this.boundingBox = new BoundingBox(new WGS84Point(latitude1, longitude1), new WGS84Point(latitude2, longitude2));
    }

    public int getGeohashFirstMatchingBits() {
        return geohashFirstMatchingBits;
    }

    public int getBitsToShift() {
        return bitsToShift;
    }

    public double getLatitude1() {
        return latitude1;
    }

    public double getLongitude1() {
        return longitude1;
    }

    public double getLatitude2() {
        return latitude2;
    }

    public double getLongitude2() {
        return longitude2;
    }

    public boolean contains(double latitude, double longitude) {
        return boundingBox.contains(new WGS84Point(latitude, longitude));
    }

    @Override
    public String toString() {
        return "GeohashBoundaryBox{" +
                ", geohashFirstMatchingBits=" + geohashFirstMatchingBits +
                ", bitsToShift=" + bitsToShift +
                ", latitude1=" + latitude1 +
                ", longitude1=" + longitude1 +
                ", latitude2=" + latitude2 +
                ", longitude2=" + longitude2 +
                '}';
    }
}
