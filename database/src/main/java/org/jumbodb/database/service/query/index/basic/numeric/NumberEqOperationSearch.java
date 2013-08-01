package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerQueryValueRetriever;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberEqOperationSearch<T, S, IFV, IF extends NumberSnappyIndexFile<IFV>> implements OperationSearch<T, IFV, IF> {
    private NumberSnappyIndexStrategy<T, IFV, IF> strategy;

    public NumberEqOperationSearch(NumberSnappyIndexStrategy<T, IFV, IF> strategy) {
        this.strategy = strategy;
    }

    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryValueRetriever queryClause, SnappyChunks snappyChunks) throws IOException {
        S searchValue = queryClause.getValue();
        return findFirstMatchingChunk(indexRaf, snappyChunks, searchValue);
    }

    protected long findFirstMatchingChunk(RandomAccessFile indexRaf, SnappyChunks snappyChunks, S searchValue) throws IOException {
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        int maxChunk = numberOfChunks - 1;
        int lastFromChunk = -1;
        int lastToChunk = -1;
        // TODO verify snappy version
        while(lastFromChunk != fromChunk && lastToChunk != toChunk) {
            int chunkDiff = (toChunk - fromChunk) / 2;
            int currentChunk = chunkDiff + fromChunk;

            byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
            T firstInt = strategy.readFirstValue(uncompressed);
            T lastInt = strategy.readLastValue(uncompressed);

            // firstInt == searchValue
            if(eq(firstInt, searchValue)) {
                // ok ist gleich ein block weiter zurück ... da es bereits da beginnen könnte
                while(currentChunk > 0) {
                    currentChunk--;
                    uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
                    firstInt = strategy.readFirstValue(uncompressed);
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
