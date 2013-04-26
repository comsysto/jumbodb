package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongLtOperationSearch;
import org.jumbodb.database.service.query.index.longval.snappy.LongNeOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeNeOperationSearch extends LongNeOperationSearch {
    protected DateTimeNeOperationSearch(NumberSnappyIndexStrategy<Long, Long, NumberSnappyIndexFile<Long>> strategy) {
        super(strategy);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DateTimeQueryValueRetriever(queryClause);
    }
}
