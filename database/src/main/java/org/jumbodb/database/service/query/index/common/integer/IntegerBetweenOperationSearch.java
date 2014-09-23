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
    public long findFirstMatchingBlock(FileDataRetriever<Integer> fileDataRetriever,
                                       QueryValueRetriever queryValueRetriever, Blocks blocks) throws IOException {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        return super.findFirstMatchingBlock(fileDataRetriever, blocks, from);
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
      NumberIndexFile<Integer> indexFile) {
        List<Integer> value = queryValueRetriever.getValue();
        Integer from = value.get(0);
        Integer to = value.get(1);
        if (from >= indexFile.getFrom() && from <= indexFile.getTo()) {
            return true;
        }
        else if(to >= indexFile.getFrom() && to <= indexFile.getTo()) {
            return true;
        }
        return from < indexFile.getFrom() && to > indexFile.getTo();
    }

    @Override
    public QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery) {
        return new IntegerBetweenQueryValueRetriever(indexQuery);
    }
}
