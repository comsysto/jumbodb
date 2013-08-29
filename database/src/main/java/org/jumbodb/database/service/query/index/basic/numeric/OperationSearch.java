package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.data.common.snappy.SnappyChunks;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public interface OperationSearch<T, IFV, IF extends NumberSnappyIndexFile<IFV>> {
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, IF snappyIndexFile);
    long findFirstMatchingChunk(FileDataRetriever<T> indexRaf, QueryValueRetriever queryValueRetriever, SnappyChunks snappyChunks) throws IOException;
    boolean matching(T currentValue, QueryValueRetriever queryValueRetriever);
    QueryValueRetriever getQueryValueRetriever(QueryClause queryClause);
}
