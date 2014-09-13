package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.FileDataRetriever;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;
import org.jumbodb.data.common.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class DoubleBetweenOperationSearch extends DoubleEqOperationSearch {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<Double> fileDataRetriever, QueryValueRetriever queryValueRetriever, SnappyChunks snappyChunks) throws IOException {
        List<Double> value = queryValueRetriever.getValue();
        Double from = value.get(0);
        return super.findFirstMatchingChunk(fileDataRetriever, snappyChunks, from);
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
        Double to = value.get(0);
        if(from < snappyIndexFile.getFrom() && from < snappyIndexFile.getTo()) {
            return true;
        }
        return from < snappyIndexFile.getTo() && to > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new DoubleBetweenQueryValueRetriever(indexQuery);
    }
}
