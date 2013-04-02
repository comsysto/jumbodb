package core.query;


import com.google.common.collect.HashMultimap;
import play.Logger;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SearchDeliveryChunkTask  {

//
//
//    @Override
//    public Integer call() throws Exception {
//        Collection<FileOffset> fileOffsets = findFileOffsets(deliveryChunk, searchQuery);
//        return findDataSetsByFileOffsets(deliveryChunk, fileOffsets, resultCallback, searchQuery);
//    }
//
//    private int findDataSetsByFileOffsets(DataDeliveryChunk dataDeliveryChunk, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
//        int numberOfResults = 0;
//        long startTime = System.currentTimeMillis();
//        HashMultimap<Integer, Long> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
//        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
//        if(searchQuery.getIndexComparision().size() == 0) {
//            Logger.info("Running scanned search");
//            for (File file : dataDeliveryChunk.getDataFiles().values()) {
//                tasks.add(retrieveDataSetsExecutor.submit(new RetrieveDataSetsTask(file, Collections.<Long>emptySet(), searchQuery, resultCallback)));
//            }
//        }
//        else {
//            Logger.info("Running indexed search");
//            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
//                File file = dataDeliveryChunk.getDataFiles().get(fileNameHash);
//                if(file == null) {
//                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
//                }
//                Set<Long> offsets = fileOffsetsMap.get(fileNameHash);
//                if(offsets.size() > 0) {
//                    tasks.add(retrieveDataSetsExecutor.submit(new RetrieveDataSetsTask(file, offsets, searchQuery, resultCallback)));
//                }
//            }
//        }
//
//        try {
//            for (Future<Integer> task : tasks) {
//                Integer results = task.get();
//                numberOfResults += results;
//            }
//            Logger.debug("findDataSetsByFileOffsets Time: " + (System.currentTimeMillis() - startTime) + "ms Threads: " + tasks.size());
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } catch (ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//        return numberOfResults;
//    }
//
//    private HashMultimap<Integer, Long> buildFileOffsetsMap(Collection<FileOffset> fileOffsets) {
//        HashMultimap<Integer, Long> result = HashMultimap.create();
//        for (FileOffset fileOffset : fileOffsets) {
//            result.put(fileOffset.getFileNameHash(), fileOffset.getOffset());
//        }
//        return result;
//    }
//
//    private Collection<FileOffset> findFileOffsets(DataDeliveryChunk dataDeliveryChunk, JumboQuery searchQuery) {
//        if(searchQuery.getIndexComparision().size() == 0) {
//            return Collections.emptyList();
//        }
//        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
//        for (JumboQuery.IndexComparision indexQuery : searchQuery.getIndexComparision()) {
//            tasks.add(indexExecutor.submit(new SearchIndexTask(dataDeliveryChunk, indexQuery, indexFileExecutor)));
//        }
//
//        try {
//            Collection<FileOffset> result = null;
//            for (Future<Set<FileOffset>> task : tasks) {
//                // .get is blocking, so we dont have to wait explicitly
//                Set<FileOffset> fileOffsets = task.get();
//                if(result == null) {
//                    result = new HashSet<FileOffset>(fileOffsets);
//                }
//                else {
//                    result.retainAll(fileOffsets);
//                }
//            }
//            return result;
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } catch (ExecutionException e) {
//            throw new RuntimeException(e);
//        }
//    }
}