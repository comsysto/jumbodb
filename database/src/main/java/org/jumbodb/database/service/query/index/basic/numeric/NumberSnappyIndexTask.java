package org.jumbodb.database.service.query.index.basic.numeric;


import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.FileOffset;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

public class NumberSnappyIndexTask<T, IFV, IF extends NumberSnappyIndexFile<IFV>> implements Callable<Set<FileOffset>> {
    private NumberSnappyIndexStrategy<T, IFV, IF> tifNumberSnappyIndexStrategy;
    private final File indexFile;
    private Set<QueryClause> clauses;

    public NumberSnappyIndexTask(NumberSnappyIndexStrategy<T, IFV, IF> tifNumberSnappyIndexStrategy, File indexFile, Set<QueryClause> clauses) {
        this.tifNumberSnappyIndexStrategy = tifNumberSnappyIndexStrategy;
        this.indexFile = indexFile;
        this.clauses = clauses;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return tifNumberSnappyIndexStrategy.searchOffsetsByClauses(indexFile, clauses);
    }
}