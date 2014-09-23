package org.jumbodb.database.service.query.index.common.geohash;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class GeohashBoundaryBoxOperationSearch extends NumberEqOperationSearch<GeohashCoords, GeohashContainer, Integer, NumberIndexFile<Integer>> {

    @Override
    public boolean matching(GeohashCoords currentValue, QueryValueRetriever queryValueRetriever) {
        if (matchingBlock(currentValue, queryValueRetriever)) {
            GeohashContainer container = queryValueRetriever.getValue();
            GeohashBoundaryBox searchValue = container.getAppropriateBoundaryBox(currentValue);
            return containsPoint(currentValue, searchValue, container);
        }
        return false;
    }

    @Override
    public boolean matchingBlock(GeohashCoords currentValue, QueryValueRetriever queryValueRetriever) {
        GeohashContainer container = queryValueRetriever.getValue();
        GeohashBoundaryBox searchValue = container.getAppropriateBoundaryBox(currentValue);
        int searchedGeohash = searchValue.getGeohashFirstMatchingBits();
        int bitsToShift = searchValue.getBitsToShift();
        return (currentValue.getGeohash() >> bitsToShift) == searchedGeohash;
    }

    protected boolean containsPoint(GeohashCoords currentValue, GeohashBoundaryBox searchValue,
      GeohashContainer container) {
        return searchValue.contains(currentValue.getLatitude(), currentValue.getLongitude());
    }

    @Override
    public boolean searchFinished(GeohashCoords currentValue, QueryValueRetriever queryValueRetriever,
      boolean resultsFound) {
        // test if geohash is still matching
        if (!resultsFound) {
            return false;
        }
        GeohashContainer container = queryValueRetriever.getValue();
        GeohashBoundaryBox searchValue = container.getAppropriateBoundaryBox(currentValue);
        int searchedGeohash = searchValue.getGeohashFirstMatchingBits();
        int bitsToShift = searchValue.getBitsToShift();
        return ((currentValue.getGeohash() >> bitsToShift) != searchedGeohash);
    }

    @Override
    public boolean eq(GeohashCoords val1, GeohashContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash == val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean lt(GeohashCoords val1, GeohashContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash < val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean gt(GeohashCoords val1, GeohashContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash > val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean ltEq(GeohashCoords val1, GeohashContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash <= val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean gtEq(GeohashCoords val1, GeohashContainer container) {
        GeohashBoundaryBox val2 = container.getAppropriateBoundaryBox(val1);
        int geohash = val1.getGeohash() >> val2.getBitsToShift();
        return geohash >= val2.getGeohashFirstMatchingBits();
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Integer> snappyIndexFile) {
        GeohashContainer searchValue = queryValueRetriever.getValue();
        for (GeohashBoundaryBox geohashBoundaryBox : searchValue.getSplittedBoxes()) {
            int geohash = geohashBoundaryBox.getGeohashFirstMatchingBits();
            int bitsToShift = geohashBoundaryBox.getBitsToShift();
            int from = snappyIndexFile.getFrom() >> bitsToShift;
            int to = snappyIndexFile.getTo() >> bitsToShift;
            if (geohash >= from && geohash <= to) {
                return true;
            }
        }
        return false;
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new GeohashQueryValueRetriever(indexQuery);
    }
}
