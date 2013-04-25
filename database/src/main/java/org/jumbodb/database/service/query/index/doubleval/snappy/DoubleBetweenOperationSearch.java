package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class DoubleBetweenOperationSearch extends DoubleEqOperationSearch {

    protected DoubleBetweenOperationSearch(NumberSnappyIndexStrategy<Double, Double, NumberSnappyIndexFile<Double>> strategy) {
        super(strategy);
    }

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryValueRetriever queryValueRetriever, SnappyChunks snappyChunks) throws IOException {
        List<Double> value = queryValueRetriever.getValue();
        Double from = value.get(0);
        return super.findFirstMatchingChunk(indexRaf, snappyChunks, from);
    }

    @Override
    public boolean matching(Double currentValue, QueryValueRetriever queryValueRetriever) {
        List<Double> value = queryValueRetriever.getValue();
        Double from = value.get(0);
        Double to = value.get(1);
        return from < currentValue && to > currentValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Double> snappyIndexFile) {
        List<Double> value = queryValueRetriever.getValue();
        Double from = value.get(0);
        return from > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(QueryClause queryClause) {
        return new DoubleBetweenQueryValueRetriever(queryClause);
    }
}
