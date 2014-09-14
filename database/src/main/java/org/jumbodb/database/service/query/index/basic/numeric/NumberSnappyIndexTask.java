package org.jumbodb.database.service.query.index.basic.numeric;


import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.FileOffset;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

public class NumberSnappyIndexTask<T, IFV, IF extends NumberSnappyIndexFile<IFV>> implements Callable<Set<FileOffset>> {
    private NumberSnappyIndexStrategy<T, IFV, IF> tifNumberSnappyIndexStrategy;
    private final File indexFile;
    private Set<IndexQuery> indexQueries;
    private int queryLimit;
    private boolean resultCacheEnabled;

    public NumberSnappyIndexTask(NumberSnappyIndexStrategy<T, IFV, IF> tifNumberSnappyIndexStrategy, File indexFile,
                                 Set<IndexQuery> indexQueries, int queryLimit, boolean resultCacheEnabled) {
        this.tifNumberSnappyIndexStrategy = tifNumberSnappyIndexStrategy;
        this.indexFile = indexFile;
        this.indexQueries = indexQueries;
        this.queryLimit = queryLimit;
        this.resultCacheEnabled = resultCacheEnabled;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return tifNumberSnappyIndexStrategy.searchOffsetsByIndexQueries(indexFile, indexQueries, queryLimit, resultCacheEnabled);
    }
}