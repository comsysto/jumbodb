package core.query;

import com.google.common.collect.HashMultimap;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import play.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 10:53 AM
 */
public class JumboSearcher {

    private final File dataPath;
    private final File indexPath;
    private Map<String, DataCollection> dataCollections;
    private ExecutorService retrieveDataSetsExecutor;
    private ExecutorService indexExecutor;
    private ExecutorService indexFileExecutor;
    private ObjectMapper jsonMapper;

    public JumboSearcher(File dataPath, File indexPath) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        retrieveDataSetsExecutor = Executors.newScheduledThreadPool(20);
        indexExecutor = Executors.newCachedThreadPool();
        indexFileExecutor = Executors.newScheduledThreadPool(30);
        this.dataCollections = IndexLoader.loadIndex(dataPath, indexPath);
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Logger.info("IndexedFileSearcher initialized for " + indexPath.getAbsolutePath());

    }

    public void restart() {
        Logger.info("IndexedFileSearcher restarting for " + indexPath.getAbsolutePath());
        this.dataCollections = IndexLoader.loadIndex(dataPath, indexPath);
        Logger.info("IndexedFileSearcher restarted for " + indexPath.getAbsolutePath());
    }

    public void stop() {
        retrieveDataSetsExecutor.shutdown();
        indexExecutor.shutdown();
        indexFileExecutor.shutdown();
    }

    public int findResultAndWriteIntoCallback(String collectionName, JumboQuery searchQuery, ResultCallback resultCallback) {
        DataCollection dataCollection = dataCollections.get(collectionName);
        if(dataCollection != null) {
            Collection<FileOffset> fileOffsets = findFileOffsets(dataCollection, searchQuery);
            return findDataSetsByFileOffsets(dataCollection, fileOffsets, resultCallback, searchQuery);
        }
        return 0;
    }

    private int findDataSetsByFileOffsets(DataCollection dataCollection, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        HashMultimap<Integer, Long> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        // CARSTEN diesen logic blog noch aufräumen
        if(searchQuery.getIndexComparision().size() == 0) {
            Logger.info("Running scanned search");
            for (File file : dataCollection.getDataFiles().values()) {
                tasks.add(retrieveDataSetsExecutor.submit(new RetrieveDataSetsTask(file, Collections.<Long>emptySet(), searchQuery, resultCallback)));
            }
        }
        else {
            Logger.info("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = dataCollection.getDataFiles().get(fileNameHash);
                if(file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<Long> offsets = fileOffsetsMap.get(fileNameHash);
                if(offsets.size() > 0) {
                    tasks.add(retrieveDataSetsExecutor.submit(new RetrieveDataSetsTask(file, offsets, searchQuery, resultCallback)));
                }
            }
        }

        try {
            for (Future<Integer> task : tasks) {
                Integer results = task.get();
                numberOfResults += results;
            }
            Logger.debug("findDataSetsByFileOffsets Time: " + (System.currentTimeMillis() - startTime) + "ms Threads: " + tasks.size());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return numberOfResults;
    }

    private HashMultimap<Integer, Long> buildFileOffsetsMap(Collection<FileOffset> fileOffsets) {
        HashMultimap<Integer, Long> result = HashMultimap.create();
        for (FileOffset fileOffset : fileOffsets) {
            result.put(fileOffset.getFileNameHash(), fileOffset.getOffset());
        }
        return result;
    }

    private Collection<FileOffset> findFileOffsets(DataCollection dataCollection, JumboQuery searchQuery) {
        if(searchQuery.getIndexComparision().size() == 0) {
            return Collections.emptyList();
        }
        List<Future<Set<FileOffset>>> tasks = new LinkedList<Future<Set<FileOffset>>>();
        for (JumboQuery.IndexComparision indexQuery : searchQuery.getIndexComparision()) {
            tasks.add(indexExecutor.submit(new SearchIndexTask(dataCollection, indexQuery, indexFileExecutor)));
        }

        try {
            Collection<FileOffset> result = null;
            for (Future<Set<FileOffset>> task : tasks) {
                // .get is blocking, so we dont have to wait explicitly
                Set<FileOffset> fileOffsets = task.get();
                if(result == null) {
                    result = new HashSet<FileOffset>(fileOffsets);
                }
                else {
                    result.retainAll(fileOffsets);
                }
            }
            return result;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }



}
