package org.jumbodb.database.service.query.index.common.longval;

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
public class LongBetweenOperationSearch extends LongEqOperationSearch {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<Long> fileDataRetriever,
      QueryValueRetriever queryValueRetriever, Blocks blocks) throws IOException {
        List<Long> value = queryValueRetriever.getValue();
        Long from = value.get(0);
        return super.findFirstMatchingChunk(fileDataRetriever, blocks, from);
    }

    @Override
    public boolean matching(Long currentValue, QueryValueRetriever queryValueRetriever) {
        List<Long> value = queryValueRetriever.getValue();
        Long from = value.get(0);
        Long to = value.get(1);
        return from <= currentValue && to >= currentValue;
    }

    @Override
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever,
      NumberIndexFile<Long> snappyIndexFile) {
        List<Long> value = queryValueRetriever.getValue();
        Long from = value.get(0);
        Long to = value.get(1);
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
        return new LongBetweenQueryValueRetriever(indexQuery);
    }
}
