package org.jumbodb.database.service.query.index.common;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.data.common.snappy.SnappyChunks;
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public interface IndexOperationSearch<T, IFV, IF extends NumberIndexFile<IFV>> {
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, IF snappyIndexFile);

    long findFirstMatchingChunk(FileDataRetriever<T> indexRaf, QueryValueRetriever queryValueRetriever,
      SnappyChunks snappyChunks) throws IOException;

    boolean matching(T currentValue, QueryValueRetriever queryValueRetriever);

    boolean matchingChunk(T currentValue, QueryValueRetriever queryValueRetriever);

    /**
     * Gets called when matching returns false and the question is if we should stop the search
     *
     * @param currentValue
     * @param queryValueRetriever
     * @param resultsFound
     * @return
     */
    boolean searchFinished(T currentValue, QueryValueRetriever queryValueRetriever, boolean resultsFound);

    QueryValueRetriever getQueryValueRetriever(IndexQuery indexQuery);
}
