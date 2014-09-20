package org.jumbodb.database.service.query.index.common.integer;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

import java.io.IOException;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class IntegerBetweenOperationSearch extends IntegerEqOperationSearch {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<Integer> fileDataRetriever,
      QueryValueRetriever queryValueRetriever, Blocks blocks) throws IOException {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        return super.findFirstMatchingChunk(fileDataRetriever, blocks, from);
    }

    @Override
    public boolean matching(Integer currentValue, QueryValueRetriever queryValueRetriever) {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        Integer to = value.get(1);
        return from <= currentValue && to >= currentValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Integer> snappyIndexFile) {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        Integer to = value.get(1);
        if (from >= snappyIndexFile.getFrom() && from <= snappyIndexFile.getTo()) {
            return true;
        }
        else if(to >= snappyIndexFile.getFrom() && to <= snappyIndexFile.getTo()) {
            return true;
        }
        return from < snappyIndexFile.getFrom() && to > snappyIndexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new IntegerBetweenQueryValueRetriever(indexQuery);
    }
}
