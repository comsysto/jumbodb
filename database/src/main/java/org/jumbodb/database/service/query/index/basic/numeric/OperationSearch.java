package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerSnappyIndexFile;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public interface OperationSearch<T extends Number, IF extends NumberSnappyIndexFile<T>> {
    public boolean acceptIndexFile(QueryClause queryClause, IF hashCodeSnappyIndexFile);
    long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryClause clause, SnappyChunks snappyChunks) throws IOException;
    boolean matching(T currentValue, QueryClause clause);
}
