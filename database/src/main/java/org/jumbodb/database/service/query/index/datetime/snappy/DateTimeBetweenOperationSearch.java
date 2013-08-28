package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongBetweenOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeBetweenOperationSearch extends LongBetweenOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DateTimeBetweenQueryValueRetriever(queryClause);
    }
}
