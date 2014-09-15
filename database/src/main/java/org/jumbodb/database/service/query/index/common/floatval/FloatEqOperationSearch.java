package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatEqOperationSearch extends NumberEqOperationSearch<Float, Float, Float, NumberIndexFile<Float>> {

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return currentValue.equals(searchValue);
    }

    @Override
    public boolean eq(Float val1, Float val2) {
        return val1.equals(val2);
    }

    @Override
    public boolean lt(Float val1, Float val2) {
        return val1 < val2;
    }

    @Override
    public boolean gt(Float val1, Float val2) {
        return val1 > val2;
    }

    @Override
    public boolean ltEq(Float val1, Float val2) {
        return val1 <= val2;
    }

    @Override
    public boolean gtEq(Float val1, Float val2) {
        return val1 >= val2;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getFrom() && searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new FloatQueryValueRetriever(indexQuery);
    }
}