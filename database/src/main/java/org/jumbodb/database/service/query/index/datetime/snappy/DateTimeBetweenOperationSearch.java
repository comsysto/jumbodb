package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongBetweenOperationSearch;

/**
 * @author Carsten Hufe
 */
public class DateTimeBetweenOperationSearch extends LongBetweenOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DateTimeBetweenQueryValueRetriever(indexQuery);
    }
}
