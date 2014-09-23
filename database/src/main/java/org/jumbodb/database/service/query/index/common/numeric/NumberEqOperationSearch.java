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
        return findFirstMatchingBlock(fileDataRetriever, blocks, searchValue);
    }

    @Override
    public boolean matchingBlock(T currentValue, QueryValueRetriever queryValueRetriever) {
        return matching(currentValue, queryValueRetriever);
    }

    @Override
    public boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound) {
        return resultsFound;
    }

    protected long findFirstMatchingBlock(FileDataRetriever<T> fileDataRetriever, Blocks blocks, S searchValue) throws IOException {
        int numberOfBlocks = blocks.getNumberOfBlocks();
        int fromBlock = 0;
        int toBlock = numberOfBlocks;
        int lastFromBlock = -1;
        int lastToBlock = -1;
        while(lastFromBlock != fromBlock && lastToBlock != toBlock) {
            int blockDiff = (toBlock - fromBlock) / 2;
            int currentBlock = blockDiff + fromBlock;

            BlockRange<T> blockRange = fileDataRetriever.getBlockRange(currentBlock);
            T firstInt = blockRange.getFirstValue();
            T lastInt = blockRange.getLastValue();

            // firstInt == searchValue
            if(eq(firstInt, searchValue)) {
                while(currentBlock > 0) {
                    currentBlock--;
                    blockRange = fileDataRetriever.getBlockRange(currentBlock);
                    firstInt = blockRange.getFirstValue();
//                    firstInt < searchValue
                    if(lt(firstInt, searchValue)) {
                        return currentBlock;
                    }
                }
//                firstInt == searchValue
                if(eq(firstInt, searchValue)) {
                    // chunk 0, erster hash ist gleich
                    return currentBlock;
                }

            }
            // firstInt <= searchValue && lastInt >= searchValue
            else if(ltEq(firstInt, searchValue) && gtEq(lastInt, searchValue)) {
                // ok firstHash == searchHash hat nicht gegriffen, aber die condition, der block den wir suchen!
                return currentBlock;
            }
//            lastInt < searchValue
            else if (lt(lastInt, searchValue)) {
                if(currentBlock == lastFromBlock && currentBlock == fromBlock) {
                    return currentBlock;
                }
                lastFromBlock = fromBlock;
                fromBlock = currentBlock;
//                firstInt > searchValue
            } else if(gt(firstInt, searchValue)) {
                if(currentBlock == lastToBlock && currentBlock == toBlock) {
                    return currentBlock;
                }
                lastToBlock = toBlock;
                toBlock = currentBlock;
            }
        }
        return fromBlock;
    }

    public abstract boolean eq(T val1, S val2);
    public abstract boolean lt(T val1, S val2);
    public abstract boolean gt(T val1, S val2);
    public abstract boolean ltEq(T val1, S val2);
    public abstract boolean gtEq(T val1, S val2);
}
