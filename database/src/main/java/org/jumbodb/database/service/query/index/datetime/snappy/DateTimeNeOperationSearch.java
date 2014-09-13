package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.IndexQuery;
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

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DateTimeQueryValueRetriever(indexQuery);
    }
}
