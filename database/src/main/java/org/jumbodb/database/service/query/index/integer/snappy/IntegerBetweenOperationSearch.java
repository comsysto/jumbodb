package org.jumbodb.database.service.query.index.integer.snappy;

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
public class IntegerBetweenOperationSearch extends IntegerEqOperationSearch {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<Integer> fileDataRetriever, QueryValueRetriever queryValueRetriever, SnappyChunks snappyChunks) throws IOException {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        return super.findFirstMatchingChunk(fileDataRetriever, snappyChunks, from);
    }

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        Integer to = value.get(1);
        return from < currentValue && to > currentValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, NumberSnappyIndexFile<Integer> snappyIndexFile) {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        Integer to = value.get(0);
        if(from < snappyIndexFile.getFrom() && from < snappyIndexFile.getTo()) {
            return true;
        }
        return from < snappyIndexFile.getTo() && to > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new IntegerBetweenQueryValueRetriever(indexQuery);
    }
}
