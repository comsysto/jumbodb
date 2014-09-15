package org.jumbodb.database.service.query.index.common.hashcode64;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.longval.LongEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class HashCode64EqOperationSearch extends LongEqOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new HashCode64QueryValueRetriever(indexQuery);
    }
}
