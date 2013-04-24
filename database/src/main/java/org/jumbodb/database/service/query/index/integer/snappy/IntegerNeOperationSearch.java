package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class IntegerNeOperationSearch extends NumberNeOperationSearch<Integer, NumberSnappyIndexFile<Integer>> {

    public IntegerNeOperationSearch(NumberSnappyIndexStrategy<Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public boolean ne(Integer val1, Integer val2) {
        return val1 != val2;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> hashCodeSnappyIndexFile) {
        Integer searchValue = queryValueRetriever.getValue();
        boolean fromNe = !searchValue.equals(hashCodeSnappyIndexFile.getFrom());
        boolean toNe = !searchValue.equals(hashCodeSnappyIndexFile.getTo());
        return fromNe || toNe;
    }

     @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
         Integer searchValue = queryValueRetriever.getValue();
        return !currentValue.equals(searchValue);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new IntegerQueryValueRetriever(queryClause);
    }
}
