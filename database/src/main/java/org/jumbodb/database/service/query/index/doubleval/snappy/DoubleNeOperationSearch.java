package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class DoubleNeOperationSearch extends NumberNeOperationSearch<Double, NumberSnappyIndexFile<Double>> {

    public DoubleNeOperationSearch(NumberSnappyIndexStrategy<Double, NumberSnappyIndexFile<Double>> strategy) {
        super(strategy);
    }

    @Override
    public boolean ne(Double val1, Double val2) {
        return !val1.equals(val2);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Double> snappyIndexFile) {
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
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DoubleQueryValueRetriever(queryClause);
    }
}
