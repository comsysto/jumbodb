package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongEqOperationSearch;
import org.jumbodb.database.service.query.index.longval.snappy.LongGtOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeGtOperationSearch extends LongGtOperationSearch {
    protected DateTimeGtOperationSearch(NumberSnappyIndexStrategy<Long, Long, NumberSnappyIndexFile<Long>> strategy) {
        super(strategy);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DateTimeQueryValueRetriever(queryClause);
    }
}
