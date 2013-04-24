package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public interface OperationSearch<T extends Number, IF extends NumberSnappyIndexFile<T>> {
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, IF hashCodeSnappyIndexFile);
    long findFirstMatchingChunk(RandomAccessFile indexRaf, QueryValueRetriever queryValueRetriever, SnappyChunks snappyChunks) throws IOException;
    boolean matching(T currentValue, QueryValueRetriever queryValueRetriever);
    QueryValueRetriever getQueryValueRetriever(QueryClause queryClause);
}
