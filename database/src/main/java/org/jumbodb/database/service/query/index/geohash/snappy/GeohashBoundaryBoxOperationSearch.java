package org.jumbodb.database.service.query.index.geohash.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBoxOperationSearch extends NumberEqOperationSearch<GeohashCoords, GeohashBoundaryBoxContainer, Integer, NumberSnappyIndexFile<Integer>> {

    @Override
    public boolean matching(GeohashCoords currentValue, QueryValueRetriever queryValueRetriever) {
        GeohashBoundaryBoxContainer container = queryValueRetriever.getValue();
        GeohashBoundaryBox searchValue = container.getAppropriateBoundaryBox(currentValue);
        int searchedGeohash = searchValue.getGeohashFirstMatchingBits();
        int bitsToShift = searchValue.getBitsToShift();
        if((currentValue.getGeohash() >> bitsToShift) == searchedGeohash) {
            return searchValue.contains(currentValue.getLatitude(), currentValue.getLongitude());
        }
        return false;
    }

    @Override
    public boolean searchFinished(GeohashCoords currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        // test if geohash is still matching
        if(!resultsFound) {
            return false;
        }
        GeohashBoundaryBoxContainer container = queryValueRetriever.getValue();
        GeohashBoundaryBox searchValue = container.getAppropriateBoundaryBox(currentValue);
        int searchedGeohash = searchValue.getGeohashFirstMatchingBits();
        int bitsToShift = searchValue.getBitsToShift();
        return ((currentValue.getGeohash() >> bitsToShift) != searchedGeohash);
    }

    @Override
    public boolean eq(GeohashCoords val1, GeohashBoundaryBoxContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash == val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean lt(GeohashCoords val1, GeohashBoundaryBoxContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash < val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean gt(GeohashCoords val1, GeohashBoundaryBoxContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash > val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean ltEq(GeohashCoords val1, GeohashBoundaryBoxContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash <= val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean gtEq(GeohashCoords val1, GeohashBoundaryBoxContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash >= val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> snappyIndexFile) {
        GeohashBoundaryBoxContainer searchValue = queryValueRetriever.getValue();
        for (GeohashBoundaryBox geohashBoundaryBox : searchValue.getSplittedBoxes()) {
            int geohash = geohashBoundaryBox.getGeohashFirstMatchingBits();
            int bitsToShift = geohashBoundaryBox.getBitsToShift();
            int from = snappyIndexFile.getFrom() >> bitsToShift;
            int to = snappyIndexFile.getTo() >> bitsToShift;
            if(geohash >= from && geohash <= to) {
                return true;
            }
        }
        return false;
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new GeohashBoundaryBoxQueryValueRetriever(queryClause);
    }
}
