package org.jumbodb.database.service.query.index.hashcode64.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.longval.snappy.LongEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class HashCode64EqOperationSearch extends LongEqOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new HashCode64QueryValueRetriever(indexQuery);
    }
}
