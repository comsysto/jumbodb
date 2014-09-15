package org.jumbodb.database.service.query.index.common.datetime;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.longval.LongNeOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeNeOperationSearch extends LongNeOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DateTimeQueryValueRetriever(indexQuery);
    }
}
