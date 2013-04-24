package org.jumbodb.database.service.query.index.hashcode.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerEqOperationSearch;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class HashCodeEqOperationSearch extends IntegerEqOperationSearch {
    public HashCodeEqOperationSearch(NumberSnappyIndexStrategy<Integer, NumberSnappyIndexFile<Integer>> strategy) {
        super(strategy);
    }

    @Override
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        int searchValue = queryClause.getValue().hashCode();
        return currentValue == searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryClause queryClause, NumberSnappyIndexFile<Integer> hashCodeSnappyIndexFile) {
        int searchValue = queryClause.getValue().hashCode();
        return searchValue >= hashCodeSnappyIndexFile.getFrom() && searchValue <= hashCodeSnappyIndexFile.getTo();
    }

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryClause queryClause, SnappyChunks snappyChunks) throws IOException {
        Integer searchValue = queryClause.getValue().hashCode();
        return findFirstMatchingChunk(indexRaf, snappyChunks, searchValue);
    }
}
