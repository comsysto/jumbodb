package org.jumbodb.database.service.query.index.hashcode.snappy;


import org.jumbodb.database.service.query.FileOffset;

import java.io.*;
import java.util.Set;
import java.util.concurrent.Callable;

public class HashCodeSnappyIndexTask implements Callable<Set<FileOffset>> {
    private final File indexFile;
    private final Set<Integer> hashes;

    public HashCodeSnappyIndexTask(File indexFile, Set<Integer> hashes) {
        this.indexFile = indexFile;
        this.hashes = hashes;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return HashCodeSnappySearchIndexUtils.searchOffsetsByHashes(indexFile, hashes);
    }
}