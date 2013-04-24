package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public interface OperationSearch<T> {
    public boolean acceptIndexFile(QueryClause queryClause, IntegerSnappyIndexFile hashCodeSnappyIndexFile);
    long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryClause clause, SnappyChunks snappyChunks) throws IOException;
    boolean matching(T currentValue, QueryClause clause);
}
