package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberNeOperationSearch<T extends Number, IF extends NumberSnappyIndexFile<T>> implements OperationSearch<T, IF> {
    private NumberSnappyIndexStrategy<T, IF> strategy;

    public NumberNeOperationSearch(NumberSnappyIndexStrategy<T, IF> strategy) {
        this.strategy = strategy;
    }

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryClause queryClause, SnappyChunks snappyChunks) throws IOException {
        T searchValue = (T)queryClause.getValue();
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        // TODO verify snappy version
        while(toChunk != 0) {
            int currentChunk = (toChunk - fromChunk) / 2;

            byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
            T firstInt = strategy.readFirstValue(uncompressed);
            T lastInt = strategy.readLastValue(uncompressed);

            // just going up
            //firstInt != searchValue || lastInt != searchValue
            if(ne(firstInt, searchValue) || ne(lastInt, searchValue)) {
                toChunk = currentChunk;
            } else {
                return currentChunk;
            }

        }
        return 0;
    }

    public abstract boolean ne(T val1, T val2);
}
