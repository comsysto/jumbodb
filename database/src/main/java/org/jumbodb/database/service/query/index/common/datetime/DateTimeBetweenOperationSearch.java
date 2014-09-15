package org.jumbodb.database.service.query.index.common.datetime;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.longval.LongBetweenOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeBetweenOperationSearch extends LongBetweenOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DateTimeBetweenQueryValueRetriever(indexQuery);
    }
}
