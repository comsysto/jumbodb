package org.jumbodb.database.service.query.definition;

import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.jumbodb.data.common.meta.ActiveProperties;
import org.jumbodb.data.common.meta.DeliveryProperties;
import org.jumbodb.data.common.meta.IndexProperties;

import java.io.*;
import java.util.*;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 2:54 PM
 */
public class CollectionDefinitionLoader {
    public static final FileFilter FOLDER_INSTANCE = (FileFilter) DirectoryFileFilter.INSTANCE;

    public static CollectionDefinition loadCollectionDefinition(File dataPath, File indexPath) {
        Map<String, Collection<DeliveryChunkDefinition>> result = new HashMap<String, Collection<DeliveryChunkDefinition>>();
        File[] dataCollectionDirectories = dataPath.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        if(dataCollectionDirectories == null) {
            return new CollectionDefinition(result);
        }
        for (File dataCollectionFolder  : dataCollectionDirectories) {
            if(!dataCollectionFolder.getName().startsWith(".")) {
                String collectionName = dataCollectionFolder.getName();
                Collection<DeliveryChunkDefinition> deliveryChunkDefinitions = new LinkedList<DeliveryChunkDefinition>();
                File[] chunkFolders = dataCollectionFolder.listFiles(FOLDER_INSTANCE);
                for (File chunkFolder : chunkFolders) {
                    DeliveryChunkDefinition deliveryChunkDefinition = getDataDataDeliveryChunk(indexPath, dataCollectionFolder, collectionName, chunkFolder.getName());
                    deliveryChunkDefinitions.add(deliveryChunkDefinition);
                }
                result.put(collectionName, deliveryChunkDefinitions);
            }
        }
        return new CollectionDefinition(result);
    }

    private static DeliveryChunkDefinition getDataDataDeliveryChunk(File indexPath, File dataCollectionFolder, String collectionName, String chunkKey) {
        String dataDeliveryKeyFolder = dataCollectionFolder.getAbsolutePath() + "/" + chunkKey + "/";
        String deliveryVersion = ActiveProperties.getActiveDeliveryVersion(new File(dataDeliveryKeyFolder + ActiveProperties.DEFAULT_FILENAME));

        String dataDeliveryVersionFolder = dataDeliveryKeyFolder + deliveryVersion + "/";
        String indexDeliveryVersionFolder = indexPath.getAbsolutePath() + "/" + collectionName + "/" + chunkKey + "/" + deliveryVersion + "/";
        return createDeliveryChunk(collectionName, chunkKey, new File(indexDeliveryVersionFolder), new File(dataDeliveryVersionFolder));
    }


    private static DeliveryChunkDefinition createDeliveryChunk(String collectionName, String chunkKey, File collectionIndexFolder, File collectionDataFolder) {
        File[] indexFolders = collectionIndexFolder.listFiles(FOLDER_INSTANCE);
        List<IndexDefinition> resIndexFiles = new LinkedList<IndexDefinition>();
        Map<Integer, File> resDataFiles = new HashMap<Integer, File>();
        if(indexFolders != null) {
            for (File indexFolder : indexFolders) {
                String strategy = getIndexStrategy(indexFolder);
                resIndexFiles.add(new IndexDefinition(indexFolder.getName(), indexFolder, strategy));
            }
        }
        AndFileFilter ands = new AndFileFilter();
        ands.addFileFilter(new NotFileFilter(new SuffixFileFilter(".properties")));
        ands.addFileFilter(new NotFileFilter(new SuffixFileFilter(".chunks.snappy")));
        ands.addFileFilter(new NotFileFilter(new SuffixFileFilter(".sha1")));
        File[] dataFiles = collectionDataFolder.listFiles((FilenameFilter) ands);
        for (File dataFile : dataFiles) {
            resDataFiles.put(dataFile.getName().hashCode(), dataFile);
        }
        return new DeliveryChunkDefinition(collectionName, chunkKey, resIndexFiles, resDataFiles, getDataStrategy(collectionDataFolder));
    }

    private static String getIndexStrategy(File indexFolder) {
        return IndexProperties.getStrategy(new File(indexFolder.getAbsolutePath() + "/" + IndexProperties.DEFAULT_FILENAME));
    }

    private static String getDataStrategy(File dataFolder) {
        return DeliveryProperties.getStrategy(new File(dataFolder.getAbsolutePath() + "/" + DeliveryProperties.DEFAULT_FILENAME));
    }
}
