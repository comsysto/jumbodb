package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.database.service.query.index.basic.numeric.*;

/**
 * @author Carsten Hufe
 */
public class IntegerGtOperationSearch extends IntegerEqOperationSearch {

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> snappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue < snappyIndexFile.getFrom() || searchValue < snappyIndexFile.getTo();
    }

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue > searchValue;
    }
}
