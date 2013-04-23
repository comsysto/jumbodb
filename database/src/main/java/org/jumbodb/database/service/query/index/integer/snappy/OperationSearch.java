package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public interface OperationSearch<T> {
    long findFirstMatchingChunk(RandomAccessFile indexRaf, Integer searchValue, SnappyChunks snappyChunks) throws IOException;
    boolean matching(T currentValue, T searchValue);
}
