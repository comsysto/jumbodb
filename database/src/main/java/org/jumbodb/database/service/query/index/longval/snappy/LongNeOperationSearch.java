package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberNeOperationSearch;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

/**
 * @author Carsten Hufe
 */
public class LongNeOperationSearch extends NumberNeOperationSearch<Long, Long, NumberSnappyIndexFile<Long>> {

    @Override
    public boolean ne(Long val1, Long val2) {
        return !val1.equals(val2);
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Long> snappyIndexFile) {
        Long searchValue = queryValueRetriever.getValue();
        boolean fromNe = !searchValue.equals(snappyIndexFile.getFrom());
        boolean toNe = !searchValue.equals(snappyIndexFile.getTo());
        return fromNe || toNe;
    }

     @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        Long searchValue = queryValueRetriever.getValue();
        return !currentValue.equals(searchValue);
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new LongQueryValueRetriever(indexQuery);
    }
}
