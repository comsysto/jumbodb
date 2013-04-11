package org.jumbodb.database.service.query;

import com.google.common.collect.HashMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class SearchIndexTask implements Callable<Set<FileOffset>> {
    private Logger log = LoggerFactory.getLogger(SearchIndexTask.class);

    private final DataDeliveryChunk dataDeliveryChunk;
    private JumboQuery.IndexComparision query;
    private ExecutorService indexFileExecutor;

    public SearchIndexTask(DataDeliveryChunk dataDeliveryChunk, JumboQuery.IndexComparision query, ExecutorService indexFileExecutor) {
        this.dataDeliveryChunk = dataDeliveryChunk;
        this.query = query;
        this.indexFileExecutor = indexFileExecutor;
    }

    @Override
    public Set<FileOffset> call() throws Exception {
        long start = System.currentTimeMillis();
        MultiValueMap<File, Integer> groupedByIndexFile = SearchIndexUtils.groupByIndexFile(dataDeliveryChunk, query);
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
        for (File indexFile : groupedByIndexFile.keySet()) {
            tasks.add(indexFileExecutor.submit(new SearchIndexFileTask(indexFile, new HashSet<Integer>(groupedByIndexFile.get(indexFile)))));
        }
        Set<FileOffset> result = new HashSet<FileOffset>();
        for (Future<Set<FileOffset>> task : tasks) {
            result.addAll(task.get());
        }
        log.info("Time for search one complete index offsets " + query.getValues().size() + ": " + (System.currentTimeMillis() - start) + "ms");
        return result;
    }



}