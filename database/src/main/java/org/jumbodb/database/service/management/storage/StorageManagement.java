package org.jumbodb.database.service.management.storage;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.importer.ImportHelper;
import org.jumbodb.database.service.management.storage.dto.collections.DeliveryChunk;
import org.jumbodb.database.service.management.storage.dto.collections.DeliveryVersion;
import org.jumbodb.database.service.management.storage.dto.collections.JumboCollection;
import org.jumbodb.database.service.management.storage.dto.deliveries.ChunkedDeliveryVersion;
import org.jumbodb.database.service.management.storage.dto.deliveries.VersionedJumboCollection;
import org.jumbodb.database.service.query.Restartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
public class StorageManagement {
    public static final FileFilter FOLDER_FILTER = FileFilterUtils.makeDirectoryOnly(FileFilterUtils.notFileFilter(FileFilterUtils.prefixFileFilter(".")));

    private final Logger log = LoggerFactory.getLogger(StorageManagement.class);

    private JumboConfiguration config;
    private Restartable queryServer;

    public StorageManagement(JumboConfiguration config, Restartable queryServer) {
        this.config = config;
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
        return new File(getIndexPath().getAbsolutePath() + "/" + collectionName);
    }

    private File findCollectionDataFolder(String collectionName) {
        return new File(getDataPath().getAbsolutePath() + "/" + collectionName);
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
            delete(new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey));
            delete(new File(getIndexPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey));
        }
    }

    private void rawDeleteChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        delete(new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + version));
        delete(new File(getIndexPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + version));
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
        File[] files = getDataPath().listFiles();
        for (File file : files) {
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
        return new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/active.properties");
    }

    private String findAppropriateInactiveVersionToActivate(String collection, String deliveryChunkKey) {
        String excludedVersion = getActiveDeliveryVersion(collection, deliveryChunkKey);
        File pathToVersions = new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + deliveryChunkKey);
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
        String date = getDate(versionFolder);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        try {
            return sdf.parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDate(File versionFolder) {
        Properties activeProps = getDeliveryProperties(versionFolder);
        return activeProps.getProperty("date");
    }

    private Properties getDeliveryProperties(File versionFolder) {
        try {
            String deliveryPropsPath = versionFolder.getAbsolutePath() + "/delivery.properties";
            return PropertiesLoaderUtils.loadProperties(new FileSystemResource(deliveryPropsPath));
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private void activateDeliveryVersion(String version, File activeDeliveryFile) {
//        System.out.println("Mock: Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
        log.info("Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
        ImportHelper.writeActiveFile(activeDeliveryFile, version);
    }

    public File getIndexPath() {
        return config.getIndexPath();
    }

    public File getDataPath() {
        return config.getDataPath();
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


    public List<JumboCollection> getJumboCollections() {
        List<JumboCollection> collections = new LinkedList<JumboCollection>();
        File[] collectionFolders = getDataPath().listFiles(FOLDER_FILTER);
        for (File collectionFolder : collectionFolders) {
            collections.add(getJumboCollection(collectionFolder));
        }
        Collections.sort(collections);
        return collections;
    }

    private JumboCollection getJumboCollection(File collectionFolder) {
        String collectionName = collectionFolder.getName();
        List<DeliveryChunk> collectionChunks = getCollectionDeliveryChunks(collectionName, collectionFolder);
        String collapseId = "col" + collectionName.hashCode();
        return new JumboCollection(collapseId, collectionName, collectionChunks);
    }

    private List<DeliveryChunk> getCollectionDeliveryChunks(String collectionName, File collectionFolder) {
        List<DeliveryChunk> deliveryChunks = new LinkedList<DeliveryChunk>();
        File[] chunkFolders = collectionFolder.listFiles(FOLDER_FILTER);
        for (File chunkFolder : chunkFolders) {
            deliveryChunks.add(getDeliveryChunk(chunkFolder, collectionName));
        }
        Collections.sort(deliveryChunks);
        return deliveryChunks;
    }

    private DeliveryChunk getDeliveryChunk(File chunkFolder, String collectionName) {
        String chunkKey = chunkFolder.getName();
        String activeVersion = getActiveDeliveryVersion(collectionName, chunkKey);
        return new DeliveryChunk(chunkKey, getDeliveryVersions(chunkFolder, collectionName, chunkKey, activeVersion));
    }

    private List<DeliveryVersion> getDeliveryVersions(File chunkFolder, String collectionName, String chunkKey, String activeVersion) {
        List<DeliveryVersion> deliveryVersions = new LinkedList<DeliveryVersion>();
        File[] deliveryVersionFolders = chunkFolder.listFiles(FOLDER_FILTER);
        for (File deliveryVersionFolder : deliveryVersionFolders) {
            deliveryVersions.add(getDeliveryVersion(deliveryVersionFolder, collectionName, chunkKey, activeVersion));
        }
        Collections.sort(deliveryVersions);
        return deliveryVersions;
    }

    private DeliveryVersion getDeliveryVersion(File deliveryVersionFolder, String collectionName, String chunkKey, String activeVersion) {
        String version = deliveryVersionFolder.getName();
        Properties deliveryProperties = getDeliveryProperties(deliveryVersionFolder);
        String info = deliveryProperties.getProperty("info");
        String date = deliveryProperties.getProperty("date");
        long compressedSize = calculateCompressedSize(deliveryVersionFolder);
        long uncompressedSize = getUncompressedSize(deliveryVersionFolder);
        long indexSize = getIndexSize(collectionName, chunkKey, version);
        boolean active = activeVersion.equals(version);
        return new DeliveryVersion(version, info, date, compressedSize, uncompressedSize, indexSize, active);
    }

    private long getUncompressedSize(File deliveryVersionFolder) {
        long uncompressedSize = 0l;
        FileFilter metaFiler = FileFilterUtils.makeFileOnly(FileFilterUtils.suffixFileFilter(".chunks.snappy"));
        File[] snappyChunks = deliveryVersionFolder.listFiles(metaFiler);
        for (File snappyChunk : snappyChunks) {
            uncompressedSize += getSizeFromSnappyChunk(snappyChunk);
        }
        return uncompressedSize;
    }

    private long getSizeFromSnappyChunk(File snappyChunk) {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(snappyChunk);
            dis = new DataInputStream(fis);
            return dis.readLong();
        } catch (FileNotFoundException e) {
            throw new UnhandledException(e);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
        finally {
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(fis);
        }
    }

    private long getIndexSize(String collectionName, String chunkKey, String version) {
        File indexVersionFolder = new File(getIndexPath().getAbsolutePath() + "/" + collectionName + "/" + chunkKey + "/" + version);
        if(indexVersionFolder.exists()) {
            return calculateCompressedSize(indexVersionFolder);
        }
        return 0l;
    }

    public List<ChunkedDeliveryVersion> getChunkedDeliveryVersions() {
        List<ChunkedDeliveryVersion> chunkedDeliveryVersions = new LinkedList<ChunkedDeliveryVersion>();
        List<VersionedJumboCollection> versionedJumboCollections = getAllVersionedJumboCollections();
        HashMultimap<ChunkKeyVersion, VersionedJumboCollection> groupByChunkKeyAndVersion = groupByChunkKeyAndVersion(versionedJumboCollections);
        Set<ChunkKeyVersion> keys = groupByChunkKeyAndVersion.keySet();
        for (ChunkKeyVersion key : keys) {
            List<VersionedJumboCollection> versions = new ArrayList<VersionedJumboCollection>(groupByChunkKeyAndVersion.get(key));
            Collections.sort(versions);
            String collapseId = "col" + key.hashCode();
            String info = findInfos(versions);
            String date = findDates(versions);
            chunkedDeliveryVersions.add(new ChunkedDeliveryVersion(collapseId, key.chunkKey, key.version, info, date, versions));
        }
        Collections.sort(chunkedDeliveryVersions);
        return chunkedDeliveryVersions;
    }

    private String findDates(List<VersionedJumboCollection> versions) {
        Set<String> dates = new HashSet<String>();
        for (VersionedJumboCollection version : versions) {
            dates.add(version.getDate());
        }
        return StringUtils.join(dates, ",");
    }

    private String findInfos(List<VersionedJumboCollection> versions) {
        Set<String> infos = new HashSet<String>();
        for (VersionedJumboCollection version : versions) {
            infos.add(version.getInfo());
        }
        return StringUtils.join(infos, " - ");
    }

    private HashMultimap<ChunkKeyVersion, VersionedJumboCollection> groupByChunkKeyAndVersion(List<VersionedJumboCollection> versionedJumboCollections) {
        HashMultimap<ChunkKeyVersion, VersionedJumboCollection> groupedByChunkKeyAndVersion = HashMultimap.create();
        for (VersionedJumboCollection versionedJumboCollection : versionedJumboCollections) {
            groupedByChunkKeyAndVersion.put(new ChunkKeyVersion(versionedJumboCollection.getChunkKey(), versionedJumboCollection.getVersion()), versionedJumboCollection);
        }
        return groupedByChunkKeyAndVersion;
    }

    private List<VersionedJumboCollection> getAllVersionedJumboCollections() {
        List<VersionedJumboCollection> versionedJumboCollections = new LinkedList<VersionedJumboCollection>();
        File[] collectionFolders = getDataPath().listFiles(FOLDER_FILTER);
        for (File collectionFolder : collectionFolders) {
            String collectionName = collectionFolder.getName();
            File[] deliveryChunkFolders = collectionFolder.listFiles(FOLDER_FILTER);
            for (File deliveryChunkFolder : deliveryChunkFolders) {
                String chunkKey = deliveryChunkFolder.getName();
                String activeVersion = getActiveDeliveryVersion(collectionName, chunkKey);
                File[] versionFolders = deliveryChunkFolder.listFiles(FOLDER_FILTER);
                for (File versionFolder : versionFolders) {
                    String version = versionFolder.getName();
                    Properties deliveryProperties = getDeliveryProperties(versionFolder);
                    String info = deliveryProperties.getProperty("info");
                    String date = deliveryProperties.getProperty("date");
                    boolean active = activeVersion.equals(version);
                    long compressedSize = calculateCompressedSize(versionFolder);
                    long uncompressedSize = getUncompressedSize(versionFolder);
                    long indexSize = getIndexSize(collectionName, chunkKey, version);
                    versionedJumboCollections.add(new VersionedJumboCollection(collectionName, version, chunkKey, info, date, active, compressedSize, uncompressedSize, indexSize));
                }
            }

        }
        return versionedJumboCollections;
    }

    private long calculateCompressedSize(File versionFolder) {
        return FileUtils.sizeOfDirectory(versionFolder);
    }


    private static class ChunkKeyVersion {
        final String chunkKey;
        final String version;

        private ChunkKeyVersion(String chunkKey, String version) {
            this.chunkKey = chunkKey;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ChunkKeyVersion that = (ChunkKeyVersion) o;

            if (!chunkKey.equals(that.chunkKey)) return false;
            if (!version.equals(that.version)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = chunkKey.hashCode();
            result = 31 * result + version.hashCode();
            return result;
        }
    }
}