package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongGtOperationSearch extends LongEqOperationSearch {
    public LongGtOperationSearch(NumberSnappyIndexStrategy<Long, NumberSnappyIndexFile<Long>> strategy) {
        super(strategy);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Long> hashCodeSnappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        return searchValue > hashCodeSnappyIndexFile.getFrom();
    }

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return currentValue > searchValue;
    }
}
