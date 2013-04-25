package org.jumbodb.database.service.query.index.floatval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberEqOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatEqOperationSearch extends NumberEqOperationSearch<Float, Float, Float, NumberSnappyIndexFile<Float>> {


    public FloatEqOperationSearch(NumberSnappyIndexStrategy<Float, Float, NumberSnappyIndexFile<Float>> strategy) {
        super(strategy);
    }

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
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        return searchValue >= snappyIndexFile.getFrom() && searchValue <= snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new FloatQueryValueRetriever(queryClause);
    }
}
