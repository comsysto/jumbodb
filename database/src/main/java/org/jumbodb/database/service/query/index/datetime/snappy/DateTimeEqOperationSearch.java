package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongBetweenOperationSearch;
import org.jumbodb.database.service.query.index.longval.snappy.LongEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeEqOperationSearch extends LongEqOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DateTimeQueryValueRetriever(queryClause);
    }
}
