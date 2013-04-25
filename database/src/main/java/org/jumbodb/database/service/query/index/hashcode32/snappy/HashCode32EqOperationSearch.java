package org.jumbodb.database.service.query.index.hashcode32.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerEqOperationSearch;

/**
 * @author Carsten Hufe
 */
public class HashCode32EqOperationSearch extends IntegerEqOperationSearch {
    public HashCode32EqOperationSearch(NumberSnappyIndexStrategy<Integer, Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new HashCode32QueryValueRetriever(queryClause);
    }
}
