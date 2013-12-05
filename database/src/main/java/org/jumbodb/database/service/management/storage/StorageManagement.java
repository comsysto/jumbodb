package org.jumbodb.database.service.management.storage;

import com.google.common.collect.HashMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.UnhandledException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.connector.importer.DataInfo;
import org.jumbodb.connector.importer.IndexInfo;
import org.jumbodb.connector.importer.MetaData;
import org.jumbodb.connector.importer.MetaIndex;
import org.jumbodb.data.common.meta.ActiveProperties;
import org.jumbodb.data.common.meta.DeliveryProperties;
import org.jumbodb.data.common.meta.IndexProperties;
import org.jumbodb.database.service.configuration.JumboConfiguration;
import org.jumbodb.database.service.importer.ImportServer;
import org.jumbodb.database.service.management.storage.dto.collections.DeliveryChunk;
import org.jumbodb.database.service.management.storage.dto.collections.DeliveryVersion;
import org.jumbodb.database.service.management.storage.dto.collections.JumboCollection;
import org.jumbodb.database.service.management.storage.dto.deliveries.ChunkedDeliveryVersion;
import org.jumbodb.database.service.management.storage.dto.deliveries.VersionedJumboCollection;
import org.jumbodb.database.service.management.storage.dto.index.CollectionIndex;
import org.jumbodb.database.service.management.storage.dto.maintenance.TemporaryFiles;
import org.jumbodb.database.service.management.storage.dto.queryutil.QueryUtilCollection;
import org.jumbodb.database.service.management.storage.dto.queryutil.QueryUtilIndex;
import org.jumbodb.database.service.query.JumboSearcher;
import org.jumbodb.database.service.query.data.DataStrategy;
import org.jumbodb.database.service.query.index.IndexStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
// CARSTEN clean up... contains implementation details of the strategies, like fetching size from compressed files!
public class StorageManagement {
    public static final FileFilter FOLDER_FILTER = FileFilterUtils.makeDirectoryOnly(FileFilterUtils.notFileFilter(FileFilterUtils.prefixFileFilter(".")));

    private final Logger log = LoggerFactory.getLogger(StorageManagement.class);

    private JumboConfiguration config;
    private JumboSearcher jumboSearcher;
    private ImportServer importServer;


    public StorageManagement(JumboConfiguration config, JumboSearcher jumboSearcher, ImportServer importServer) {
        this.config = config;
        this.jumboSearcher = jumboSearcher;
        this.importServer = importServer;
    }

    private void onDataChanged() {
        jumboSearcher.onDataChanged();
    }

    public TemporaryFiles getMaintenanceTemporaryFilesInfo() {
        File tempDataPath = getTempDataPath();
        long dataSize = 0l;
        if(tempDataPath.exists()) {
            dataSize = FileUtils.sizeOfDirectory(tempDataPath);
        }
        File tempIndexPath = getTempIndexPath();
        long indexSize = 0l;
        if(tempIndexPath.exists()) {
            indexSize = FileUtils.sizeOfDirectory(tempIndexPath);
        }
        String sizeFormated = FileUtils.byteCountToDisplaySize(dataSize + indexSize);
        File[] files = tempDataPath.listFiles(FOLDER_FILTER);
        return new TemporaryFiles(sizeFormated, files != null ? files.length : 0, importServer.isImportRunning());
    }

    public void maintenanceCleanupTemporaryFiles() {
        try {
            if(importServer.isImportRunning()) {
               throw new IllegalStateException("Import is running, so cleanup is not possible!");
            }
            FileUtils.deleteDirectory(getTempDataPath());
            FileUtils.deleteDirectory(getTempIndexPath());
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private File getTempIndexPath() {
        return new File(config.getIndexPath().getAbsolutePath() + "/.tmp/");
    }

    private File getTempDataPath() {
        return new File(config.getDataPath().getAbsolutePath() + "/.tmp/");
    }

    public List<QueryUtilCollection> findQueryableCollections() {
        List<QueryUtilCollection> result = new LinkedList<QueryUtilCollection>();
        List<JumboCollection> jumboCollections = getJumboCollections(false);
        String dataStrategyName = "none";
        Set<QueryOperation> supportedOperations = new HashSet<QueryOperation>();
        for (JumboCollection collection : jumboCollections) {
            Set<QueryUtilIndex> resultIndexes = new HashSet<QueryUtilIndex>();
            for (DeliveryChunk deliveryChunk : collection.getChunks()) {
                for (DeliveryVersion deliveryVersion : deliveryChunk.getVersions()) {
                    List<CollectionIndex> collectionIndexes = getCollectionIndexes(collection.getName(), deliveryChunk.getKey(), deliveryVersion.getVersion());
                    for (CollectionIndex collectionIndex : collectionIndexes) {
                        IndexStrategy indexStrategy = jumboSearcher.getIndexStrategy(collectionIndex.getStrategy());
                        resultIndexes.add(new QueryUtilIndex(collectionIndex.getIndexName(), collectionIndex.getStrategy(), new ArrayList<QueryOperation>(indexStrategy.getSupportedOperations())));
                    }
                    DataStrategy dataStrategy = jumboSearcher.getDataStrategy(collection.getName(), deliveryChunk.getKey());
                    dataStrategyName = dataStrategy.getStrategyName();
                    supportedOperations.addAll(dataStrategy.getSupportedOperations());
                }
            }
            result.add(new QueryUtilCollection(collection.getName(), new ArrayList<QueryUtilIndex>(resultIndexes), dataStrategyName, new ArrayList<QueryOperation>(supportedOperations)));
        }
        return result;
    }

    public void deleteCompleteCollection(String collectionName) {
        log.info("deleteCompleteCollection (" + collectionName + ")");
        // nothing to activate, because collection is away
        deleteCompleteCollectionWithoutRestart(collectionName);
        onDataChanged();
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
        onDataChanged();
    }

    public void deleteChunkedVersionInCollection(String collection, String chunkDeliveryKey, String version) {
        log.info("deleteChunkedVersionInCollection (" + collection + ", " + chunkDeliveryKey + ", " + version + ")");
        deleteChunkedVersionInCollectionWithoutRestart(collection, chunkDeliveryKey, version);
        onDataChanged();

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
        onDataChanged();
    }

    private List<String> findCollectionsWithChunkAndVersion(String chunkedDeliveryKey, String version) {
        List<File> collectionDirectories = findCollectionDataDirectories();
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

    private List<File> findCollectionDataDirectories() {
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
        onDataChanged();
    }

    private void activateChunkedVersionInCollectionWithoutRestart(String collection, String chunkDeliveryKey, String version) {
        File activeDeliveryFile = getActiveDeliveryFile(collection, chunkDeliveryKey);
        activateDeliveryVersion(version, activeDeliveryFile);
    }


    public String getActiveDeliveryVersion(String collection, String chunkDeliveryKey) {
        return ActiveProperties.getActiveDeliveryVersion(getActiveDeliveryFile(collection, chunkDeliveryKey));
    }

    private File getActiveDeliveryFile(String collection, String chunkDeliveryKey) {
        return new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + ActiveProperties.DEFAULT_FILENAME);
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
        return DeliveryProperties.getDate(getDeliveryPropertiesFile(versionFolder));
    }

    private void activateDeliveryVersion(String version, File activeDeliveryFile) {
//        System.out.println("Mock: Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
        log.info("Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
        ActiveProperties.writeActiveFile(activeDeliveryFile, version);
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
        return getJumboCollections(true);
    }

    public List<JumboCollection> getJumboCollections(boolean loadSizes) {
        List<JumboCollection> collections = new LinkedList<JumboCollection>();
        File[] collectionFolders = getDataPath().listFiles(FOLDER_FILTER);
        for (File collectionFolder : collectionFolders) {
            collections.add(getJumboCollection(collectionFolder, loadSizes));
        }
        Collections.sort(collections);
        return collections;
    }

    private JumboCollection getJumboCollection(File collectionFolder, boolean loadSizes) {
        String collectionName = collectionFolder.getName();
        List<DeliveryChunk> collectionChunks = getCollectionDeliveryChunks(collectionName, collectionFolder, loadSizes);
        String collapseId = "col" + collectionName.hashCode();
        return new JumboCollection(collapseId, collectionName, collectionChunks);
    }

    private List<DeliveryChunk> getCollectionDeliveryChunks(String collectionName, File collectionFolder, boolean loadSizes) {
        List<DeliveryChunk> deliveryChunks = new LinkedList<DeliveryChunk>();
        File[] chunkFolders = collectionFolder.listFiles(FOLDER_FILTER);
        for (File chunkFolder : chunkFolders) {
            deliveryChunks.add(getDeliveryChunk(chunkFolder, collectionName, loadSizes));
        }
        Collections.sort(deliveryChunks);
        return deliveryChunks;
    }

    private DeliveryChunk getDeliveryChunk(File chunkFolder, String collectionName, boolean loadSizes) {
        String chunkKey = chunkFolder.getName();
        String activeVersion = getActiveDeliveryVersion(collectionName, chunkKey);
        return new DeliveryChunk(chunkKey, getDeliveryVersions(chunkFolder, collectionName, chunkKey, activeVersion, loadSizes));
    }

    private List<DeliveryVersion> getDeliveryVersions(File chunkFolder, String collectionName, String chunkKey, String activeVersion, boolean loadSizes) {
        List<DeliveryVersion> deliveryVersions = new LinkedList<DeliveryVersion>();
        File[] deliveryVersionFolders = chunkFolder.listFiles(FOLDER_FILTER);
        for (File deliveryVersionFolder : deliveryVersionFolders) {
            deliveryVersions.add(getDeliveryVersion(deliveryVersionFolder, collectionName, chunkKey, activeVersion, loadSizes));
        }
        Collections.sort(deliveryVersions);
        return deliveryVersions;
    }

    private DeliveryVersion getDeliveryVersion(File deliveryVersionFolder, String collectionName, String chunkKey, String activeVersion, boolean loadSizes) {
        String version = deliveryVersionFolder.getName();
        DeliveryProperties.DeliveryMeta deliveryMeta = DeliveryProperties.getDeliveryMeta(getDeliveryPropertiesFile(deliveryVersionFolder));
        long compressedSize = loadSizes ? calculateCompressedSize(deliveryVersionFolder) : 0l;
        long uncompressedSize = loadSizes ? getUncompressedSize(deliveryVersionFolder) : 0l;
        long indexSize = loadSizes ? getIndexSize(collectionName, chunkKey, version) : 0l;
        boolean active = activeVersion.equals(version);
        return new DeliveryVersion(version, deliveryMeta.getInfo(), dateToString(deliveryMeta.getDate()), compressedSize, uncompressedSize, indexSize, active);
    }

    private File getDeliveryPropertiesFile(File deliveryVersionFolder) {
        return new File(deliveryVersionFolder.getAbsolutePath() + "/" + DeliveryProperties.DEFAULT_FILENAME);
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
                    DeliveryProperties.DeliveryMeta meta = DeliveryProperties.getDeliveryMeta(getDeliveryPropertiesFile(versionFolder));
                    boolean active = activeVersion.equals(version);
                    long compressedSize = calculateCompressedSize(versionFolder);
                    long uncompressedSize = getUncompressedSize(versionFolder);
                    long indexSize = getIndexSize(collectionName, chunkKey, version);
                    versionedJumboCollections.add(new VersionedJumboCollection(collectionName, version, chunkKey, meta.getInfo(), dateToString(meta.getDate()), meta.getSourcePath(), meta.getStrategy(), active, compressedSize, uncompressedSize, indexSize));
                }
            }

        }
        return versionedJumboCollections;
    }

    private long calculateCompressedSize(File versionFolder) {
        return FileUtils.sizeOfDirectory(versionFolder);
    }

    public List<MetaData> getMetaDataForDelivery(String deliveryChunkKey, String version, boolean activate) {
        List<VersionedJumboCollection> allVersionedJumboCollections = getAllVersionedJumboCollections();
        List<MetaData> result = new LinkedList<MetaData>();
        for (VersionedJumboCollection collection : allVersionedJumboCollections) {
            if(deliveryChunkKey.equals(collection.getChunkKey()) && version.equals(collection.getVersion())) {
                result.add(new MetaData(collection.getCollectionName(), deliveryChunkKey, version, collection.getStrategy(), collection.getSourcePath(), activate, collection.getInfo()));
            }
        }
        return result;
    }

    public List<MetaIndex> getMetaIndexForDelivery(String deliveryChunkKey, String version) {
        List<VersionedJumboCollection> allVersionedJumboCollections = getAllVersionedJumboCollections();
        List<MetaIndex> result = new LinkedList<MetaIndex>();
        for (VersionedJumboCollection collection : allVersionedJumboCollections) {
            if(deliveryChunkKey.equals(collection.getChunkKey()) && version.equals(collection.getVersion())) {
                List<CollectionIndex> collectionIndexes = getCollectionIndexes(collection.getCollectionName(), deliveryChunkKey, version);
                for (CollectionIndex collectionIndex : collectionIndexes) {
                    result.add(new MetaIndex(collection.getCollectionName(), deliveryChunkKey, version, collectionIndex.getIndexName(), collectionIndex.getStrategy(), collectionIndex.getIndexSourceFields()));
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    private List<CollectionIndex> getCollectionIndexes(String collectionName, String deliveryChunkKey, String version) {
        File collectionVersionIndexPath = findCollectionChunkedVersionIndexFolder(collectionName, deliveryChunkKey, version);
        File[] indexFolders = collectionVersionIndexPath.listFiles(FOLDER_FILTER);
        if(indexFolders == null) {
            return Collections.emptyList();
        }
        List<CollectionIndex> result = new LinkedList<CollectionIndex>();
        for (File indexFolder : indexFolders) {
            IndexProperties.IndexMeta props = IndexProperties.getIndexMeta(new File(indexFolder.getAbsolutePath() + "/" + IndexProperties.DEFAULT_FILENAME));
            result.add(new CollectionIndex(indexFolder.getName(), dateToString(props.getDate()), props.getIndexSourceFields(), props.getStrategy()));
        }
        return result;
    }

    private String dateToString(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(DeliveryProperties.DATE_PATTERN);
        return sdf.format(date);
    }

    private File findCollectionChunkedVersionIndexFolder(String collectionName, String deliveryChunkKey, String version) {
        return new File(getIndexPath().getAbsolutePath() + "/" + collectionName + "/" + deliveryChunkKey + "/" + version + "/");
    }

    private File findCollectionChunkedVersionIndexFolder(String collectionName, String deliveryChunkKey, String version, String indexName) {
        return new File(getIndexPath().getAbsolutePath() + "/" + collectionName + "/" + deliveryChunkKey + "/" + version + "/" + indexName + "/");
    }

    private File findCollectionChunkedVersionDataFolder(String collectionName, String deliveryChunkKey, String version) {
        return new File(getDataPath().getAbsolutePath() + "/" + collectionName + "/" + deliveryChunkKey + "/" + version + "/");
    }

    public List<IndexInfo> getIndexInfoForDelivery(List<MetaIndex> metaIndex) {
        List<IndexInfo> result = new LinkedList<IndexInfo>();
        for (MetaIndex index : metaIndex) {
            // TODO fix me implementation details of strategy
            File indexFolder = findCollectionChunkedVersionIndexFolder(index.getCollection(), index.getDeliveryKey(), index.getDeliveryVersion(), index.getIndexName());
            FilenameFilter ioFileFilter = FileFilterUtils.suffixFileFilter(".odx");
            File[] files = indexFolder.listFiles(ioFileFilter);
            for (File indexFile : files) {
                long fileLength = getSizeFromSnappyChunk(new File(indexFile.getAbsolutePath() + ".chunks.snappy"));
                result.add(new IndexInfo(index.getCollection(), index.getIndexName(), indexFile.getName(), fileLength, index.getDeliveryKey(), index.getDeliveryVersion(), index.getIndexStrategy()));
            }
        }
        Collections.sort(result);
        return result;
    }

    // TODO fix me implementation details of strategy
    public List<DataInfo> getDataInfoForDelivery(List<MetaData> metaDatas) {
        List<DataInfo> result = new LinkedList<DataInfo>();
        IOFileFilter notChunksSnappy = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".chunks.snappy"));
        IOFileFilter notProperties = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".properties"));
        IOFileFilter notSha1 = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".sha1"));
        IOFileFilter notMd5 = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".md5"));
        FilenameFilter ioFileFilter = FileFilterUtils.and(notChunksSnappy, notProperties, notSha1, notMd5);
        for (MetaData data : metaDatas) {
            File dataFolder = findCollectionChunkedVersionDataFolder(data.getCollection(), data.getDeliveryKey(), data.getDeliveryVersion());
            File[] files = dataFolder.listFiles(ioFileFilter);
            for (File dataFile : files) {
                long fileLength = getSizeFromSnappyChunk(new File(dataFile.getAbsolutePath() + ".chunks.snappy"));
                result.add(new DataInfo(data.getCollection(), dataFile.getName(), fileLength, data.getDeliveryKey(), data.getDeliveryVersion(), data.getDataStrategy()));
            }
        }
        return result;
    }

    public InputStream getInputStream(IndexInfo index) throws IOException {
        File indexFolder = findCollectionChunkedVersionIndexFolder(index.getCollection(), index.getDeliveryKey(), index.getDeliveryVersion(), index.getIndexName());
        File indexFile = new File(indexFolder.getAbsolutePath() + "/" + index.getFilename());
        return new SnappyInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
    }

    public InputStream getInputStream(DataInfo data) throws IOException {
        File dataFolder = findCollectionChunkedVersionDataFolder(data.getCollection(), data.getDeliveryKey(), data.getDeliveryVersion());
        File dataFile = new File(dataFolder.getAbsolutePath() + "/" + data.getFilename());
        return new SnappyInputStream(new BufferedInputStream(new FileInputStream(dataFile)));
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
