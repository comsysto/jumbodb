package org.jumbodb.database.service.query.index.geohash.snappy;

/**
 * @author Carsten Hufe
 */
public class GeohashCoords {
    private int geohash;
    private float latitude;
    private float longitude;

    public GeohashCoords(int geohash, float latitude, float longitude) {
        this.geohash = geohash;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public int getGeohash() {
        return geohash;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    @Override
    public String toString() {
        return "GeohashCoords{" +
                "geohash=" + geohash +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
}
