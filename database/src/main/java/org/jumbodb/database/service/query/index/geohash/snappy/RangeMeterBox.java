package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.geo.geohash.WGS84Point;

/**
 * @author Carsten Hufe
 */
public class RangeMeterBox {
    private double latitude;
    private double longitude;
    private double rangeInMeter;

    public RangeMeterBox(double latitude, double longitude, double rangeInMeter) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.rangeInMeter = rangeInMeter;
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
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", rangeInMeter=" + rangeInMeter +
                '}';
    }
}
