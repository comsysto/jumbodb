package org.jumbodb.database.service.query.index.common.datetime;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.longval.LongGtOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeGtOperationSearch extends LongGtOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DateTimeQueryValueRetriever(indexQuery);
    }
}
