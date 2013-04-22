package org.jumbodb.database.service.query.data.snappy;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.importer.ImportMetaFileInformation;
import org.jumbodb.database.service.query.FileOffset;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.definition.CollectionDefinition;
import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class JsonSnappyDataStrategy implements DataStrategy {
    public static final int SNAPPY_DATA_CHUNK_SIZE = 32 * 1024;
    public static final String JSON_SNAPPY_V1 = "JSON_SNAPPY_V1";
    private Logger log = LoggerFactory.getLogger(JsonSnappyDataStrategy.class);

    private ExecutorService retrieveDataExecutor;

    @Override
    public void onImport(ImportMetaFileInformation information, InputStream dataInputStream, File absoluteImportPathFile) {
        String absoluteImportPath = absoluteImportPathFile.getAbsolutePath() + "/";
        OutputStream sos = null;
        DataOutputStream dos = null;
        BufferedOutputStream bos = null;
        FileOutputStream snappyChunksFos = null;
        DataOutputStream snappyChunksDos = null;
        try {
            File storageFolderFile = new File(absoluteImportPath);
            if (!storageFolderFile.exists()) {
                storageFolderFile.mkdirs();
            }
            String filePlacePath = absoluteImportPath + information.getFileName();
            File filePlacePathFile = new File(filePlacePath);
            if (filePlacePathFile.exists()) {
                filePlacePathFile.delete();
            }
            log.info("ImportServer - " + filePlacePath);

//                        if (information.getFileType() == ImportMetaFileInformation.FileType.DATA) {
            String filePlaceChunksPath = filePlacePath + ".chunks.snappy";
            File filePlaceChunksFile = new File(filePlaceChunksPath);
            if (filePlaceChunksFile.exists()) {
                filePlaceChunksFile.delete();
            }
            snappyChunksFos = new FileOutputStream(filePlaceChunksFile);
            snappyChunksDos = new DataOutputStream(snappyChunksFos);
            final DataOutputStream finalSnappyChunksDos = snappyChunksDos;

            snappyChunksDos.writeLong(information.getFileLength());
            snappyChunksDos.writeInt(SNAPPY_DATA_CHUNK_SIZE);
            // CARSTEN pfui, cleanup when time!
            bos = new BufferedOutputStream(new FileOutputStream(filePlacePathFile)) {
                @Override
                public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                    finalSnappyChunksDos.writeInt(i2);
                    super.write(bytes, i, i2);
                }
            };
            sos = new SnappyOutputStream(bos, SNAPPY_DATA_CHUNK_SIZE);
            IOUtils.copy(dataInputStream, sos);
            sos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(bos);
            IOUtils.closeQuietly(sos);
            IOUtils.closeQuietly(snappyChunksDos);
            IOUtils.closeQuietly(snappyChunksFos);
            IOUtils.closeQuietly(dataInputStream);
        }
    }

    @Override
    public boolean isResponsibleFor(String collection, String chunkKey) {
        return true;
    }

    @Override
    public String getStrategyName() {
        return JSON_SNAPPY_V1;
    }

    @Override
    public int findDataSetsByFileOffsets(DeliveryChunkDefinition deliveryChunkDefinition, Collection<FileOffset> fileOffsets, ResultCallback resultCallback, JumboQuery searchQuery) {
        int numberOfResults = 0;
        long startTime = System.currentTimeMillis();
        HashMultimap<Integer, Long> fileOffsetsMap = buildFileOffsetsMap(fileOffsets);
        List<Future<Integer>> tasks = new LinkedList<Future<Integer>>();
        if (searchQuery.getIndexQuery().size() == 0) {
            log.info("Running scanned search");
            for (File file : deliveryChunkDefinition.getDataFiles().values()) {
                tasks.add(retrieveDataExecutor.submit(new JsonSnappyRetrieveDataSetsTask(file, Collections.<Long>emptySet(), searchQuery, resultCallback)));
            }
        } else {
            log.info("Running indexed search");
            for (Integer fileNameHash : fileOffsetsMap.keySet()) {
                File file = deliveryChunkDefinition.getDataFiles().get(fileNameHash);
                if (file == null) {
                    throw new IllegalStateException("File with " + fileNameHash + " not found!");
                }
                Set<Long> offsets = fileOffsetsMap.get(fileNameHash);
                if (offsets.size() > 0) {
                    tasks.add(retrieveDataExecutor.submit(new JsonSnappyRetrieveDataSetsTask(file, offsets, searchQuery, resultCallback)));
                }
            }
        }

        try {
            for (Future<Integer> task : tasks) {
                Integer results = task.get();
                numberOfResults += results;
            }
            log.debug("findDataSetsByFileOffsets Time: " + (System.currentTimeMillis() - startTime) + "ms Threads: " + tasks.size());
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

    @Override
    public List<QueryOperation> getSupportedOperations() {
        return Arrays.asList(QueryOperation.EQ);
    }

    @Override
    public void onInitialize(CollectionDefinition collectionDefinition) {
    }

    @Override
    public void onDataChanged(CollectionDefinition collectionDefinition) {
    }

    @Required
    public void setRetrieveDataExecutor(ExecutorService retrieveDataExecutor) {
        this.retrieveDataExecutor = retrieveDataExecutor;
    }
}
