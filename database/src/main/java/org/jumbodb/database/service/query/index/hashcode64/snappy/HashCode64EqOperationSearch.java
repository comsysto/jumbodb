package org.jumbodb.database.service.query.index.hashcode64.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class HashCode64EqOperationSearch extends LongEqOperationSearch {
    protected HashCode64EqOperationSearch(NumberSnappyIndexStrategy<Long, Long, NumberSnappyIndexFile<Long>> strategy) {
        super(strategy);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new HashCode64QueryValueRetriever(queryClause);
    }
}
