package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerEqOperationSearch implements OperationSearch<Integer> {
    @Override
    public long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryClause queryClause, SnappyChunks snappyChunks) throws IOException {
        int searchValue = (Integer)queryClause.getValue();
        return findFirstMatchingChunk(indexRaf, snappyChunks, searchValue);
    }

    protected long findFirstMatchingChunk(RandomAccessFile indexRaf, SnappyChunks snappyChunks, int searchValue) throws IOException {
        int numberOfChunks = snappyChunks.getNumberOfChunks();
        int fromChunk = 0;
        int toChunk = numberOfChunks;
        int maxChunk = numberOfChunks - 1;
        int lastFromChunk = -1;
        int lastToChunk = -1;
        // TODO verify snappy version
        while((lastFromChunk != fromChunk && lastToChunk != toChunk) || fromChunk == maxChunk) {
            int chunkDiff = (toChunk - fromChunk) / 2;
            int currentChunk = chunkDiff + fromChunk;

            byte[] uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
            int firstInt = IntegerSnappySearchIndexUtils.readFirstInt(uncompressed);
            int lastInt = IntegerSnappySearchIndexUtils.readLastInt(uncompressed);

            if(firstInt == searchValue) {
                // ok ist gleich ein block weiter zurück ... da es bereits da beginnen könnte
                while(currentChunk > 0) {
                    currentChunk--;
                    uncompressed = SnappyUtil.getUncompressed(indexRaf, snappyChunks, currentChunk);
                    firstInt = IntegerSnappySearchIndexUtils.readFirstInt(uncompressed);
                    if(firstInt < searchValue) {
                        return currentChunk;
                    }
                }
                if(firstInt == searchValue) {
                    // chunk 0, erster hash ist gleich
                    return currentChunk;
                }

            }
            else if(firstInt <= searchValue && lastInt >= searchValue) {
                // ok firstHash == searchHash hat nicht gegriffen, aber die condition, der block den wir suchen!
                return currentChunk;
            }
            else if (lastInt < searchValue) {
                lastFromChunk = fromChunk;
                fromChunk = currentChunk;
            } else if(firstInt > searchValue) {
                lastToChunk = toChunk;
                toChunk = currentChunk;
            }
        }
        return 0;
    }

    @Override
    public boolean matching(Integer currentValue, QueryClause queryClause) {
        int searchValue = (Integer)queryClause.getValue();
        return currentValue == searchValue;
    }

    @Override
    public boolean acceptIndexFile(QueryClause queryClause, IntegerSnappyIndexFile hashCodeSnappyIndexFile) {
        int searchValue = (Integer)queryClause.getValue();
        return searchValue >= hashCodeSnappyIndexFile.getFromInt() && searchValue <= hashCodeSnappyIndexFile.getToInt();
    }
}
