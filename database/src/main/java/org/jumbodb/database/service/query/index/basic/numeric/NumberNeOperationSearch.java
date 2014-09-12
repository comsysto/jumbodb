package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberNeOperationSearch<T, IFV, IF extends NumberSnappyIndexFile<IFV>> implements OperationSearch<T, IFV, IF> {

    @Override
    public long findFirstMatchingChunk(FileDataRetriever<T> fileDataRetriever, QueryValueRetriever queryClause, SnappyChunks snappyChunks) throws IOException {
        T searchValue = (T)queryClause.getValue();
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        // TODO verify snappy version
        while(toChunk != 0) {
            int currentChunk = (toChunk - fromChunk) / 2;
            BlockRange<T> blockRange = fileDataRetriever.getBlockRange(currentChunk);
            T firstInt = blockRange.getFirstValue();
            T lastInt = blockRange.getLastValue();

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

    @Override
    public boolean matchingChunk(T currentValue, QueryValueRetriever queryValueRetriever) {
        return matching(currentValue, queryValueRetriever);
    }

    @Override
    public boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        return resultsFound;
    }

    public abstract boolean ne(T val1, T val2);
}
