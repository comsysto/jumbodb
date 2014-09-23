package org.jumbodb.database.service.query.index.common.numeric;

import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.index.common.BlockRange;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public abstract class NumberEqOperationSearch<T, S, IFV, IF extends NumberIndexFile<IFV>> implements IndexOperationSearch<T, IFV, IF> {
    @Override
    public long findFirstMatchingBlock(FileDataRetriever<T> fileDataRetriever, QueryValueRetriever queryClause, Blocks blocks) throws IOException {
        S searchValue = queryClause.getValue();
        return findFirstMatchingChunk(fileDataRetriever, blocks, searchValue);
    }

    @Override
    public boolean matchingBlock(T currentValue, QueryValueRetriever queryValueRetriever) {
        return matching(currentValue, queryValueRetriever);
    }

    @Override
    public boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        return resultsFound;
    }

    // CARSTEN move to NumberSnappyIndexFile and call by delegate
    protected long findFirstMatchingChunk(FileDataRetriever<T> fileDataRetriever, Blocks blocks, S searchValue) throws IOException {
        int numberOfChunks = blocks.getNumberOfBlocks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        int lastFromChunk = -1;
        int lastToChunk = -1;
        while(lastFromChunk != fromChunk && lastToChunk != toChunk) {
            int chunkDiff = (toChunk - fromChunk) / 2;
            int currentChunk = chunkDiff + fromChunk;

            BlockRange<T> blockRange = fileDataRetriever.getBlockRange(currentChunk);
            T firstInt = blockRange.getFirstValue();
            T lastInt = blockRange.getLastValue();

            // firstInt == searchValue
            if(eq(firstInt, searchValue)) {
                while(currentChunk > 0) {
                    currentChunk--;
                    blockRange = fileDataRetriever.getBlockRange(currentChunk);
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
