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
//            byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
//            T firstInt = strategy.readFirstValue(uncompressed);
//            T lastInt = strategy.readLastValue(uncompressed);
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

    public abstract boolean ne(T val1, T val2);
}
