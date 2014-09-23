package org.jumbodb.database.service.query.index.common.integer;

import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

/**
 * @author Carsten Hufe
 */
public class IntegerGtEqOperationSearch extends IntegerEqOperationSearch {

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberIndexFile<Integer> indexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue <= indexFile.getFrom() || searchValue <= indexFile.getTo();
    }

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue >= searchValue;
    }
}
