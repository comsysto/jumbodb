package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.GeoHash;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBoxQueryValueRetriever implements QueryValueRetriever {
    private GeohashBoundaryBox value;

    public GeohashBoundaryBoxQueryValueRetriever(QueryClause queryClause) {
        List<List<Number>> coords = (List<List<Number>>) queryClause.getValue();
        double latitude1 = coords.get(0).get(0).doubleValue();
        double longitude1 = coords.get(0).get(1).doubleValue();
        double latitude2 = coords.get(1).get(0).doubleValue();
        double longitude2 = coords.get(1).get(1).doubleValue();
        GeoHash geoHash1 = GeoHash.withBitPrecision(latitude1, longitude1, 32);
        GeoHash geoHash2 = GeoHash.withBitPrecision(latitude2, longitude2, 32);
        int geoHashInt1 = geoHash1.intValue();
        int geoHashInt2 = geoHash2.intValue();
        int bitsToShift = GeoHash.getBitsToShift(geoHashInt1, geoHashInt2);
        value = new GeohashBoundaryBox(geoHashInt1 >> bitsToShift, bitsToShift, latitude1, longitude1, latitude2, longitude2);
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
