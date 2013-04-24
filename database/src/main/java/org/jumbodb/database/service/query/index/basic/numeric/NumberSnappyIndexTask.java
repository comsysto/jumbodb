package org.jumbodb.database.service.query.index.basic.numeric;


import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.FileOffset;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

public class NumberSnappyIndexTask<T extends Number, IF extends NumberSnappyIndexFile<T>> implements Callable<Set<FileOffset>> {
    private NumberSnappyIndexStrategy<T, IF> tifNumberSnappyIndexStrategy;
    private final File indexFile;
    private Set<QueryClause> clauses;

    public NumberSnappyIndexTask(NumberSnappyIndexStrategy<T, IF> tifNumberSnappyIndexStrategy, File indexFile, Set<QueryClause> clauses) {
        this.tifNumberSnappyIndexStrategy = tifNumberSnappyIndexStrategy;
        this.indexFile = indexFile;
        this.clauses = clauses;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return tifNumberSnappyIndexStrategy.searchOffsetsByClauses(indexFile, clauses);
    }
}