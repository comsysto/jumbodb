package org.jumbodb.database.service.query.definition;

import org.apache.commons.io.filefilter.*;
import org.jumbodb.data.common.meta.ActiveProperties;
import org.jumbodb.data.common.meta.CollectionProperties;
import org.jumbodb.data.common.meta.IndexProperties;
import org.springframework.util.LinkedMultiValueMap;

import java.io.*;
import java.util.*;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 2:54 PM
 */
public class CollectionDefinitionLoader {
    public static final FileFilter FOLDER_INSTANCE = (FileFilter) DirectoryFileFilter.INSTANCE;
    public static final AndFileFilter FILE_FILTER = new AndFileFilter();

    static {
        FILE_FILTER.addFileFilter(new NotFileFilter(new PrefixFileFilter("_")));
        FILE_FILTER.addFileFilter(new NotFileFilter(new PrefixFileFilter(".")));
        FILE_FILTER.addFileFilter(new NotFileFilter(new SuffixFileFilter(".properties")));
        FILE_FILTER.addFileFilter(new NotFileFilter(new SuffixFileFilter(".blocks")));
        FILE_FILTER.addFileFilter(new NotFileFilter(new SuffixFileFilter(".sha1")));
        FILE_FILTER.addFileFilter(new NotFileFilter(new SuffixFileFilter(".md5")));
    }

    public static CollectionDefinition loadCollectionDefinition(File dataPath, File indexPath) {
        File[] deliveryKeyFolders = dataPath.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        if(deliveryKeyFolders == null) {
            return new CollectionDefinition(new HashMap<String, List<DeliveryChunkDefinition>>());
        }
        LinkedMultiValueMap<String, DeliveryChunkDefinition> deliveryChunkDefinitions = new LinkedMultiValueMap<String, DeliveryChunkDefinition>();
        for (File deliveryKeyFolder : deliveryKeyFolders) {
            if(!deliveryKeyFolder.getName().startsWith(".")) {
                String deliveryKey = deliveryKeyFolder.getName();
                File activeProperties = new File(deliveryKeyFolder.getAbsolutePath() + "/" + ActiveProperties.DEFAULT_FILENAME);
                if(ActiveProperties.isDeliveryActive(activeProperties)) {
                    String activeVersion = ActiveProperties.getActiveDeliveryVersion(activeProperties);
                    File activeVersionFolder = new File(deliveryKeyFolder.getAbsolutePath() + "/" + activeVersion + "/");
                    if(activeVersionFolder.exists()) {
                        File[] collectionFolders = activeVersionFolder.listFiles(FOLDER_INSTANCE);
                        for (File collectionFolder : collectionFolders) {
                            String collectionName = collectionFolder.getName();
                            File indexDeliveryVersionFolder = new File(indexPath.getAbsolutePath() + "/" + deliveryKey + "/" + activeVersion + "/" + collectionName + "/");
                            DeliveryChunkDefinition dataDataDeliveryChunk = createDeliveryChunk(deliveryKey, collectionName, indexDeliveryVersionFolder, collectionFolder);
                            deliveryChunkDefinitions.add(collectionName, dataDataDeliveryChunk);
                        }
                    }
                    else {
                        throw new IllegalStateException("Active version '" + activeVersionFolder + "' does not exist for delivery '" + deliveryKey + "'.");
                    }
                }
            }
        }
        return new CollectionDefinition(deliveryChunkDefinitions);
    }

    private static DeliveryChunkDefinition createDeliveryChunk(String chunkKey, String collectionName, File collectionIndexFolder, File collectionDataFolder) {
        CollectionProperties.CollectionMeta collectionMeta = CollectionProperties.getCollectionMeta(new File(collectionDataFolder.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME));
        File[] indexFolders = collectionIndexFolder.listFiles(FOLDER_INSTANCE);
        List<IndexDefinition> resIndexFiles = new LinkedList<IndexDefinition>();
        Map<Integer, File> resDataFiles = new HashMap<Integer, File>();
        if(indexFolders != null) {
            for (File indexFolder : indexFolders) {
                String strategy = getIndexStrategy(indexFolder);
                resIndexFiles.add(new IndexDefinition(indexFolder.getName(), indexFolder, strategy));
            }
        }
        File[] dataFiles = collectionDataFolder.listFiles((FilenameFilter) FILE_FILTER);
        for (File dataFile : dataFiles) {
            resDataFiles.put(dataFile.getName().hashCode(), dataFile);
        }
        Collections.sort(resIndexFiles);
        return new DeliveryChunkDefinition(chunkKey, collectionName, collectionMeta.getDateFormat(), resIndexFiles, resDataFiles, getDataStrategy(collectionDataFolder));
    }

    private static String getIndexStrategy(File indexFolder) {
        return IndexProperties.getStrategy(new File(indexFolder.getAbsolutePath() + "/" + IndexProperties.DEFAULT_FILENAME));
    }

    private static String getDataStrategy(File dataFolder) {
        return CollectionProperties.getStrategy(new File(dataFolder.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME));
    }
}
