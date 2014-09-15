package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongLtOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeLtOperationSearch extends LongLtOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DateTimeQueryValueRetriever(indexQuery);
    }
}
