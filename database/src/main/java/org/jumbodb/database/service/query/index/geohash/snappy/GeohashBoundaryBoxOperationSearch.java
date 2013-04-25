package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.geo.geohash.BoundingBox;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerEqOperationSearch;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerQueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBoxOperationSearch extends NumberEqOperationSearch<GeohashCoords, GeohashBoundaryBox, Integer, NumberSnappyIndexFile<Integer>> {


    public GeohashBoundaryBoxOperationSearch(NumberSnappyIndexStrategy<GeohashCoords, Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public boolean matching(GeohashCoords currentValue, QueryValueRetriever queryValueRetriever) {
        GeohashBoundaryBox searchValue = queryValueRetriever.getValue();
        int searchedGeohash = searchValue.getGeohashFirstMatchingBits();
        int bitsToShift = searchValue.getBitsToShift();
        if((currentValue.getGeohash() >> bitsToShift) == searchedGeohash) {
            return searchValue.contains(currentValue.getLatitude(), currentValue.getLongitude());
        }
        return false;
    }

    @Override
    public boolean eq(GeohashCoords val1, GeohashBoundaryBox val2) {
        return val1.equals(val2);
    }

    @Override
    public boolean lt(GeohashCoords val1, GeohashBoundaryBox val2) {
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash < val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean gt(GeohashCoords val1, GeohashBoundaryBox val2) {
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash > val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean ltEq(GeohashCoords val1, GeohashBoundaryBox val2) {
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash <= val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean gtEq(GeohashCoords val1, GeohashBoundaryBox val2) {
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash >= val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> snappyIndexFile) {
        GeohashBoundaryBox searchValue = queryValueRetriever.getValue();
        int geohash = searchValue.getGeohashFirstMatchingBits();
        int bitsToShift = searchValue.getBitsToShift();
        int from = snappyIndexFile.getFrom() >> bitsToShift;
        int to = snappyIndexFile.getTo() >> bitsToShift;
        System.out.println("Geohash     " + Integer.toBinaryString(geohash));
        System.out.println("from        " + Integer.toBinaryString(snappyIndexFile.getFrom()));
        System.out.println("to          " + Integer.toBinaryString(snappyIndexFile.getTo()));
        System.out.println("from        " + Integer.toBinaryString(from));
        System.out.println("to          " + Integer.toBinaryString(to));
        return geohash >= from && geohash <= to;
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new GeohashQueryValueRetriever(queryClause);
    }
}
