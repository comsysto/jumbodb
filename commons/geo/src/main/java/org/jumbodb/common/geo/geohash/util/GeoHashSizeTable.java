package org.jumbodb.common.geo.geohash.util;


import org.jumbodb.common.geo.geohash.BoundingBox;

public class GeoHashSizeTable {
    private static final int NUM_BITS = 64;
    private static final double[] dLat = new double[NUM_BITS];
    private static final double[] dLon = new double[NUM_BITS];

    static {
        for (int i = 0; i < NUM_BITS; i++) {
            dLat[i] = dLat(i);
            dLon[i] = dLon(i);
        }
    }

    protected static final double dLat(int bits) {
        return 180d / Math.pow(2, bits / 2);
    }

    protected static final double dLon(int bits) {
        return 360d / Math.pow(2, (bits + 1) / 2);
    }

    public static final int numberOfBitsForOverlappingGeoHash(BoundingBox boundingBox) {
        int bits = 63;
        double height = boundingBox.getLatitudeSize();
        double width = boundingBox.getLongitudeSize();
        while ((dLat[bits] < height || dLon[bits] < width) && bits > 0) {
            bits--;
        }
        return bits;
    }
}