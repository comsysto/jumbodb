package core.query;

import com.google.common.collect.HashMultimap;
import core.query.OlchingQuery;
import play.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SearchIndexTask implements Callable<Set<FileOffset>> {
    private final DataCollection dataCollection;
    private OlchingQuery.IndexComparision query;
    private ExecutorService indexFileExecutor;

    public SearchIndexTask(DataCollection dataCollection, OlchingQuery.IndexComparision query, ExecutorService indexFileExecutor) {
        this.dataCollection = dataCollection;
        this.query = query;
        this.indexFileExecutor = indexFileExecutor;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        long start = System.currentTimeMillis();
        HashMultimap<File, Integer> groupedByIndexFile = SearchIndexUtils.groupByIndexFile(dataCollection, query);
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
        for (File indexFile : groupedByIndexFile.keySet()) {
            tasks.add(indexFileExecutor.submit(new SearchIndexFileTask(indexFile, groupedByIndexFile.get(indexFile))));
        }
        Set<FileOffset> result = new HashSet<FileOffset>();
        for (Future<Set<FileOffset>> task : tasks) {
            result.addAll(task.get());
        }
        Logger.info("Time for search one complete index offsets " + query.getValues().size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }



}