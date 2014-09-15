package org.jumbodb.database.service.query.index.common.floatval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class FloatNeOperationSearch extends NumberNeOperationSearch<Float, Float, NumberIndexFile<Float>> {

    @Override
    public boolean ne(Float val1, Float val2) {
        return !val1.equals(val2);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Float> snappyIndexFile) {
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
