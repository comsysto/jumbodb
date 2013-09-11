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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeohashCoords that = (GeohashCoords) o;

        if (geohash != that.geohash) return false;
        if (Float.compare(that.latitude, latitude) != 0) return false;
        if (Float.compare(that.longitude, longitude) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = geohash;
        result = 31 * result + (latitude != +0.0f ? Float.floatToIntBits(latitude) : 0);
        result = 31 * result + (longitude != +0.0f ? Float.floatToIntBits(longitude) : 0);
        return result;
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
