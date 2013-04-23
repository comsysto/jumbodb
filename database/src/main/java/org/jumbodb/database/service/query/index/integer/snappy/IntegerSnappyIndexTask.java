package org.jumbodb.database.service.query.index.integer.snappy;


import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.FileOffset;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Callable;

public class IntegerSnappyIndexTask implements Callable<Set<FileOffset>> {
    private final File indexFile;
    private Set<QueryClause> clauses;

    public IntegerSnappyIndexTask(File indexFile, Set<QueryClause> clauses) {
        this.indexFile = indexFile;
        this.clauses = clauses;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return IntegerSnappySearchIndexUtils.searchOffsetsByClauses(indexFile, clauses);
    }
}