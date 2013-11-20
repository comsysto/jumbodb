package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.data.common.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberEqOperationSearch<T, S, IFV, IF extends NumberSnappyIndexFile<IFV>> implements OperationSearch<T, IFV, IF> {
    @Override
    public long findFirstMatchingChunk(FileDataRetriever<T> fileDataRetriever, QueryValueRetriever queryClause, SnappyChunks snappyChunks) throws IOException {
        S searchValue = queryClause.getValue();
        return findFirstMatchingChunk(fileDataRetriever, snappyChunks, searchValue);
    }

    @Override
    public boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        return resultsFound;
    }

    protected long findFirstMatchingChunk(FileDataRetriever<T> fileDataRetriever, SnappyChunks snappyChunks, S searchValue) throws IOException {
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
//        int maxChunk = numberOfChunks - 1;
        int lastFromChunk = -1;
        int lastToChunk = -1;
        // TODO verify snappy version
        while(lastFromChunk != fromChunk && lastToChunk != toChunk) {
            int chunkDiff = (toChunk - fromChunk) / 2;
            int currentChunk = chunkDiff + fromChunk;

            BlockRange<T> blockRange = fileDataRetriever.getBlockRange(currentChunk);
//            byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
            T firstInt = blockRange.getFirstValue();
            T lastInt = blockRange.getLastValue();

            // firstInt == searchValue
            if(eq(firstInt, searchValue)) {
                // ok ist gleich ein block weiter zurück ... da es bereits da beginnen könnte
                while(currentChunk > 0) {
                    currentChunk--;
                    blockRange = fileDataRetriever.getBlockRange(currentChunk);

//                    uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
//                    firstInt = strategy.readFirstValue(uncompressed);
                    firstInt = blockRange.getFirstValue();
//                    firstInt < searchValue
                    if(lt(firstInt, searchValue)) {
                        return currentChunk;
                    }
                }
//                firstInt == searchValue
                if(eq(firstInt, searchValue)) {
                    // chunk 0, erster hash ist gleich
                    return currentChunk;
                }

            }
            // firstInt <= searchValue && lastInt >= searchValue
            else if(ltEq(firstInt, searchValue) && gtEq(lastInt, searchValue)) {
                // ok firstHash == searchHash hat nicht gegriffen, aber die condition, der block den wir suchen!
                return currentChunk;
            }
//            lastInt < searchValue
            else if (lt(lastInt, searchValue)) {
                if(currentChunk == lastFromChunk && currentChunk == fromChunk) {
                    return currentChunk;
                }
                lastFromChunk = fromChunk;
                fromChunk = currentChunk;
//                firstInt > searchValue
            } else if(gt(firstInt, searchValue)) {
                if(currentChunk == lastToChunk && currentChunk == toChunk) {
                    return currentChunk;
                }
                lastToChunk = toChunk;
                toChunk = currentChunk;
            }
        }
        return fromChunk;
    }

    public abstract boolean eq(T val1, S val2);
    public abstract boolean lt(T val1, S val2);
    public abstract boolean gt(T val1, S val2);
    public abstract boolean ltEq(T val1, S val2);
    public abstract boolean gtEq(T val1, S val2);
}
