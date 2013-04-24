package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class IntegerBetweenOperationSearch extends IntegerEqOperationSearch {

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryClause queryClause, SnappyChunks snappyChunks) throws IOException {
        List<Integer> value = (List<Integer>) queryClause.getValue();
        Integer from = value.get(0);
        return super.findFirstMatchingChunk(indexRaf, snappyChunks, from);
    }

    @Override
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        List<Integer> value = (List<Integer>) queryClause.getValue();
        Integer from = value.get(0);
        Integer to = value.get(1);
        return from < currentValue && to > currentValue;
    }

    @Override
    public boolean acceptIndexFile(QueryClause queryClause, IntegerSnappyIndexFile hashCodeSnappyIndexFile) {
        List<Integer> value = (List<Integer>) queryClause.getValue();
        Integer from = value.get(0);
//        Integer to = value.get(1);
        return from > hashCodeSnappyIndexFile.getFromInt();
    }
}
