package org.jumbodb.database.service.query.index.common.doubleval;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleNeOperationSearch extends NumberNeOperationSearch<Double, Double, NumberIndexFile<Double>> {

    @Override
    public boolean ne(Double val1, Double val2) {
        return !val1.equals(val2);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Double> snappyIndexFile) {
        Double searchValue = queryValueRetriever.getValue();
        boolean fromNe = !searchValue.equals(snappyIndexFile.getFrom());
        boolean toNe = !searchValue.equals(snappyIndexFile.getTo());
        return fromNe || toNe;
    }

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        Double searchValue = queryValueRetriever.getValue();
        return !currentValue.equals(searchValue);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DoubleQueryValueRetriever(indexQuery);
    }
}
