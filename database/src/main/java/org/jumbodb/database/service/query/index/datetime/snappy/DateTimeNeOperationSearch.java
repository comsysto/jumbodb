package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
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
