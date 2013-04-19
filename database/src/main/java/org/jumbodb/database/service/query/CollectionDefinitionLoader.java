package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;

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
        Properties activeProps = loadProperties(new File(dataDeliveryKeyFolder + "active.properties"));
        String deliveryVersion = activeProps.getProperty("deliveryVersion");

        String dataDeliveryVersionFolder = dataDeliveryKeyFolder + deliveryVersion + "/";
        String indexDeliveryVersionFolder = indexPath.getAbsolutePath() + "/" + collectionName + "/" + chunkKey + "/" + deliveryVersion + "/";
        return createDeliveryChunk(collectionName, chunkKey, new File(indexDeliveryVersionFolder), new File(dataDeliveryVersionFolder));
    }

    public static Properties loadProperties(File file) {
        Properties props = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            props.load(fis);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fis);
        }
        return props;

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
        File[] dataFiles = collectionDataFolder.listFiles((FilenameFilter) ands);
        for (File dataFile : dataFiles) {
            resDataFiles.put(dataFile.getName().hashCode(), dataFile);
        }
        return new DeliveryChunkDefinition(collectionName, chunkKey, resIndexFiles, resDataFiles);
    }

    private static String getIndexStrategy(File indexFolder) {
        Properties properties = loadProperties(new File(indexFolder.getAbsolutePath() + "/index.properties"));
        return properties.getProperty("strategy");
    }
}
