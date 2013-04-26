package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.WGS84Point;

/**
 * @author Carsten Hufe
 */
public class GeohashRangeMeterBox {
//    private int geohash;
    private int geohashFirstMatchingBits;
    private int bitsToShift;
    private double latitude;
    private double longitude;
    private double rangeInMeter;

    public GeohashRangeMeterBox(int geohashFirstMatchingBits, int bitsToShift, double latitude, double longitude, double rangeInMeter) {
        this.geohashFirstMatchingBits = geohashFirstMatchingBits;
        this.bitsToShift = bitsToShift;
        this.latitude = latitude;
        this.longitude = longitude;
        this.rangeInMeter = rangeInMeter;
    }

    public int getGeohashFirstMatchingBits() {
        return geohashFirstMatchingBits;
    }

    public int getBitsToShift() {
        return bitsToShift;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getRangeInMeter() {
        return rangeInMeter;
    }

    @Override
    public String toString() {
        return "GeohashRangeMeterBox{" +
                "geohashFirstMatchingBits=" + geohashFirstMatchingBits +
                ", bitsToShift=" + bitsToShift +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", rangeInMeter=" + rangeInMeter +
                '}';
    }
}
