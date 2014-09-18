package org.jumbodb.database.service.query.index.common.longval;

import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

/**
 * @author Carsten Hufe
 */
public class LongGtEqOperationSearch extends LongEqOperationSearch {

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberIndexFile<Long> snappyIndexFile) {
        Long searchValue = queryValueRetriever.getValue();
        return searchValue <= snappyIndexFile.getFrom() || searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Long searchValue = queryValueRetriever.getValue();
        return currentValue >= searchValue;
    }
}
