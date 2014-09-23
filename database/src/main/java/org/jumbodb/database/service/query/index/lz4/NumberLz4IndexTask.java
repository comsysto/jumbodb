package org.jumbodb.database.service.query.index.lz4;


import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

public class NumberLz4IndexTask<T, IFV, IF extends NumberIndexFile<IFV>> implements Callable<Set<FileOffset>> {
    private NumberLz4IndexStrategy<T, IFV, IF> tifNumberLz4IndexStrategy;
    private final File indexFile;
    private Set<IndexQuery> indexQueries;
    private int queryLimit;
    private boolean resultCacheEnabled;

    public NumberLz4IndexTask(NumberLz4IndexStrategy<T, IFV, IF> tifNumberLz4IndexStrategy, File indexFile,
                              Set<IndexQuery> indexQueries, int queryLimit, boolean resultCacheEnabled) {
        this.tifNumberLz4IndexStrategy = tifNumberLz4IndexStrategy;
        this.indexFile = indexFile;
        this.indexQueries = indexQueries;
        this.queryLimit = queryLimit;
        this.resultCacheEnabled = resultCacheEnabled;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return tifNumberLz4IndexStrategy.searchOffsetsByIndexQueries(indexFile, indexQueries, queryLimit, resultCacheEnabled);
    }
}