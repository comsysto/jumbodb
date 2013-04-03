package org.jumbodb.database.service.management;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.jumbodb.database.service.importer.ImportHelper;
import org.jumbodb.database.service.query.Restartable;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
// CARSTEN make spring bean
public class StorageManagement {
    private org.slf4j.Logger log = LoggerFactory.getLogger(StorageManagement.class);

    private File dataPath;
    private File indexPath;
    private Restartable queryServer;

    public StorageManagement(File dataPath, File indexPath, Restartable queryServer) {
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.queryServer = queryServer;
    }

    public void deleteCompleteCollection(String collectionName) {
        log.info("deleteCompleteCollection (" + collectionName + ")");
        // nothing to activate, because collection is away
        deleteCompleteCollectionWithoutRestart(collectionName);
        queryServer.restart();
    }

    private void deleteCompleteCollectionWithoutRestart(String collectionName) {
        File collectionDataFolder = findCollectionDataFolder(collectionName);
        delete(collectionDataFolder);
        File collectionIndexFolder = findCollectionIndexFolder(collectionName);
        delete(collectionIndexFolder);
    }

    private File findCollectionIndexFolder(String collectionName) {
        return new File(indexPath.getAbsolutePath() + "/" + collectionName);
    }

    private File findCollectionDataFolder(String collectionName) {
        return new File(dataPath.getAbsolutePath() + "/" + collectionName);
    }

    public void deleteChunkedVersionForAllCollections(String chunkedDeliveryKey, String version) {
        log.info("deleteChunkedVersionForAllCollections (" + chunkedDeliveryKey + ", " + version + ")");
        // activate other collections if active
        List<String> collectionsWithChunkAndVersion = findCollectionsWithChunkAndVersion(chunkedDeliveryKey, version);
        for (String collection : collectionsWithChunkAndVersion) {
            deleteChunkedVersionInCollectionWithoutRestart(collection, chunkedDeliveryKey, version);
        }
        queryServer.restart();
    }

    public void deleteChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        log.info("deleteChunkedVersionInCollection (" + collection + ", " + chunkDeliveryKey + ", " + version + ")");
        deleteChunkedVersionInCollectionWithoutRestart(collection, chunkDeliveryKey, version);
        queryServer.restart();

    }

    private void deleteChunkedVersionInCollectionWithoutRestart(String collection, String chunkDeliveryKey, String version) {
        // activate another version in the same chunk and collection, if active
        String activeDeliveryVersion = getActiveDeliveryVersion(collection, chunkDeliveryKey);
        if(StringUtils.equals(version, activeDeliveryVersion)) {
            // version to delete is active, so activate another one
            String versionToActivate = findAppropriateInactiveVersionToActivate(collection, chunkDeliveryKey);
            if(versionToActivate != null) {
                activateChunkedVersionInCollectionWithoutRestart(collection, chunkDeliveryKey, versionToActivate);
                rawDeleteChunkedVersionInCollection(collection, chunkDeliveryKey, version);
            }
            else {
                // found no alternative we should delete the whole collection!
                deleteCompleteChunkWithoutRestart(collection, chunkDeliveryKey);
            }
        }
        else {
            // nothing active just delete
            rawDeleteChunkedVersionInCollection(collection, chunkDeliveryKey, version);
        }
    }

    private void deleteCompleteChunkWithoutRestart(String collection, String chunkDeliveryKey) {
        File collectionDataFolder = findCollectionDataFolder(collection);
        File[] chunks = collectionDataFolder.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY);
        if(chunks.length == 1) {
            // only one chunk, delete collection
            deleteCompleteCollectionWithoutRestart(collection);
        }
        else {
            delete(new File(dataPath.getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey));
            delete(new File(indexPath.getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey));
        }
    }

    private void rawDeleteChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        delete(new File(dataPath.getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + version));
        delete(new File(indexPath.getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + version));
    }

    public void activateChunkedVersionForAllCollections(String chunkedDeliveryKey, String version) {
        log.info("activateChunkedVersionForAllCollections (" + chunkedDeliveryKey + ", " + version + ")");
        List<String> matchingCollections = findCollectionsWithChunkAndVersion(chunkedDeliveryKey, version);
        for (String matchingCollection : matchingCollections) {
            activateChunkedVersionInCollectionWithoutRestart(matchingCollection, chunkedDeliveryKey, version);
        }
        queryServer.restart();
    }

    private List<String> findCollectionsWithChunkAndVersion(String chunkedDeliveryKey, String version) {
        List<File> collectionDirectories = findCollectionDirectories();
        List<String> result = new LinkedList<String>();
        for (File collectionDirectory : collectionDirectories) {
            String path = collectionDirectory.getAbsolutePath() + "/" + chunkedDeliveryKey + "/" + version;
            File pathFile = new File(path);
            if(pathFile.exists()) {
                result.add(collectionDirectory.getName());
            }
        }

        return result;
    }

    private List<File> findCollectionDirectories() {
        List<File> collectionDirectories = new LinkedList<File>();
        for (File file : dataPath.listFiles()) {
            if(!file.getName().startsWith(".") && file.isDirectory()) {
                collectionDirectories.add(file);
            }
        }
        return collectionDirectories;
    }

    public void activateChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        log.info("activateChunkedVersionInCollection (" + collection + ", " + chunkDeliveryKey + ", " + version + ")");

        activateChunkedVersionInCollectionWithoutRestart(collection, chunkDeliveryKey, version);
        queryServer.restart();
    }

    private void activateChunkedVersionInCollectionWithoutRestart(String collection, String chunkDeliveryKey, String version) {
        File activeDeliveryFile = getActiveDeliveryFile(collection, chunkDeliveryKey);
        activateDeliveryVersion(version, activeDeliveryFile);
    }


    public String getActiveDeliveryVersion(String collection, String chunkDeliveryKey) {
        return ImportHelper.getActiveDeliveryVersion(getActiveDeliveryFile(collection, chunkDeliveryKey));
    }

    private File getActiveDeliveryFile(String collection, String chunkDeliveryKey) {
        return new File(dataPath.getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/active.properties");
    }

    private String findAppropriateInactiveVersionToActivate(String collection, String deliveryChunkKey) {
        String excludedVersion = getActiveDeliveryVersion(collection, deliveryChunkKey);
        File pathToVersions = new File(dataPath.getAbsolutePath() + "/" + collection + "/" + deliveryChunkKey);
        File[] versionFolders = pathToVersions.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
        SortedMap<Date, String> possibleVersions = new TreeMap<Date, String>();
        for (File versionFolder : versionFolders) {
            String version = versionFolder.getName();
            if(!StringUtils.equals(version, excludedVersion)) {
                possibleVersions.put(getDateForVersion(versionFolder), version);
            }
        }
        if(!possibleVersions.isEmpty()) {
            Date date = possibleVersions.lastKey();
            return possibleVersions.get(date);
        }
        return null;
    }

    private Date getDateForVersion(File versionFolder) {
        String deliveryPropsPath = versionFolder.getAbsolutePath() + "/delivery.properties";
        String date = getDate(deliveryPropsPath);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            return sdf.parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDate(String deliveryPropsPath) {
        try {
            Properties activeProps = PropertiesLoaderUtils.loadProperties(new FileSystemResource(new File(deliveryPropsPath)));
            return activeProps.getProperty("date");
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private void activateDeliveryVersion(String version, File activeDeliveryFile) {
//        System.out.println("Mock: Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
        log.info("Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
        ImportHelper.writeActiveFile(activeDeliveryFile, version);
    }


    private void delete(File file) {
//        System.out.println("Mock Delete: " + file.getAbsolutePath());
        log.info("Delete: " + file.getAbsolutePath());
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
