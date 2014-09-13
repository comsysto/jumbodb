package org.jumbodb.database.service.query.index.floatval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatNeOperationSearch extends NumberNeOperationSearch<Float, Float, NumberSnappyIndexFile<Float>> {

    @Override
    public boolean ne(Float val1, Float val2) {
        return !val1.equals(val2);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Float> snappyIndexFile) {
        Float searchValue = queryValueRetriever.getValue();
        boolean fromNe = !searchValue.equals(snappyIndexFile.getFrom());
        boolean toNe = !searchValue.equals(snappyIndexFile.getTo());
        return fromNe || toNe;
    }

     @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        Float searchValue = queryValueRetriever.getValue();
        return !currentValue.equals(searchValue);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new FloatQueryValueRetriever(indexQuery);
    }
}
