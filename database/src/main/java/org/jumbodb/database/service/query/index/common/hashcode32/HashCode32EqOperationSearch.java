package org.jumbodb.database.service.query.index.common.hashcode32;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.integer.IntegerEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class HashCode32EqOperationSearch extends IntegerEqOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new HashCode32QueryValueRetriever(indexQuery);
    }
}
