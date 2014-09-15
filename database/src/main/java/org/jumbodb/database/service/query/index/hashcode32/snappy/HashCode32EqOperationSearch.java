package org.jumbodb.database.service.query.index.hashcode32.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class HashCode32EqOperationSearch extends IntegerEqOperationSearch {

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new HashCode32QueryValueRetriever(indexQuery);
    }
}
