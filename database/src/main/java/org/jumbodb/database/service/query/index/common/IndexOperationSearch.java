package org.jumbodb.database.service.query.index.common;

import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.data.common.compression.Blocks;
import org.jumbodb.database.service.query.index.common.numeric.FileDataRetriever;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public interface IndexOperationSearch<T, IFV, IF extends NumberIndexFile<IFV>> {
    public boolean acceptIndexFile(QueryValueRetriever queryValueRetriever, IF snappyIndexFile);

    long findFirstMatchingBlock(FileDataRetriever<T> indexRaf, QueryValueRetriever queryValueRetriever,
                                Blocks blocks) throws IOException;

    boolean matching(T currentValue, QueryValueRetriever queryValueRetriever);

    boolean matchingBlock(T currentValue, QueryValueRetriever queryValueRetriever);

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
