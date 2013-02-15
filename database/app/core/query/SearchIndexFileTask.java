package core.query;


import java.io.*;
import java.util.Set;
import java.util.concurrent.Callable;

public class SearchIndexFileTask implements Callable<Set<FileOffset>> {
    private final File indexFile;
    private final Set<Integer> hashes;

    public SearchIndexFileTask(File indexFile, Set<Integer> hashes) {
        this.indexFile = indexFile;
        this.hashes = hashes;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        return SearchIndexUtils.searchOffsetsByHashes(indexFile, hashes);
    }
}