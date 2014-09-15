package org.jumbodb.database.service.query.index.common.integer;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerNeOperationSearch extends NumberNeOperationSearch<Integer, Integer, NumberIndexFile<Integer>> {

    @Override
    public boolean ne(Integer val1, Integer val2) {
        return !val1.equals(val2);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Integer> snappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        boolean fromNe = !searchValue.equals(snappyIndexFile.getFrom());
        boolean toNe = !searchValue.equals(snappyIndexFile.getTo());
        return fromNe || toNe;
    }

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        Integer searchValue = queryValueRetriever.getValue();
        return !currentValue.equals(searchValue);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new IntegerQueryValueRetriever(indexQuery);
    }
}
