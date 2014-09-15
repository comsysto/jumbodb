package org.jumbodb.database.service.query.index.floatval.snappy;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.database.service.query.index.basic.numeric.FileDataRetriever;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.io.IOException;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class FloatBetweenOperationSearch extends FloatEqOperationSearch {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<Float> fileDataRetriever,
      QueryValueRetriever queryValueRetriever, SnappyChunks snappyChunks) throws IOException {
        List<Float> value = queryValueRetriever.getValue();
        Float from = value.get(0);
        return super.findFirstMatchingChunk(fileDataRetriever, snappyChunks, from);
    }

    @Override
    public boolean matching(Float currentValue, QueryValueRetriever queryValueRetriever) {
        List<Float> value = queryValueRetriever.getValue();
        Float from = value.get(0);
        Float to = value.get(1);
        return from < currentValue && to > currentValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberSnappyIndexFile<Float> snappyIndexFile) {
        List<Float> value = queryValueRetriever.getValue();
        Float from = value.get(0);
        Float to = value.get(0);
        if (from < snappyIndexFile.getFrom() && from < snappyIndexFile.getTo()) {
            return true;
        }
        return from < snappyIndexFile.getTo() && to > snappyIndexFile.getFrom();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new FloatBetweenQueryValueRetriever(indexQuery);
    }
}
