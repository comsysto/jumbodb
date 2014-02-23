package org.jumbodb.database.service.management.storage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.meta.ActiveProperties;
import org.jumbodb.data.common.meta.CollectionProperties;
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
import org.springframework.util.LinkedMultiValueMap;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
// CARSTEN fix unit tests
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
        if (tempDataPath.exists()) {
            dataSize = FileUtils.sizeOfDirectory(tempDataPath);
        }
        File tempIndexPath = getTempIndexPath();
        long indexSize = 0l;
        if (tempIndexPath.exists()) {
            indexSize = FileUtils.sizeOfDirectory(tempIndexPath);
        }
        String sizeFormated = FileUtils.byteCountToDisplaySize(dataSize + indexSize);
        File[] files = tempDataPath.listFiles(FOLDER_FILTER);
        return new TemporaryFiles(sizeFormated, files != null ? files.length : 0, importServer.isImportRunning());
    }

    public void maintenanceCleanupTemporaryFiles() {
        try {
            if (importServer.isImportRunning()) {
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
            List<QueryUtilIndex> indexes = new ArrayList<QueryUtilIndex>(resultIndexes);
            Collections.sort(indexes);
            result.add(new QueryUtilCollection(collection.getName(), indexes, dataStrategyName, new ArrayList<QueryOperation>(supportedOperations)));
        }
        return result;
    }

    public void deleteChunkedVersion(String chunkedDeliveryKey, String version) {
        log.info("deleteChunkedVersion (" + chunkedDeliveryKey + ", " + version + ")");
        // activate other collections if active
        // CARSTEN implement correctly
        // CARSTEN active other version
        // CARSTEN delete chunk folder if last one...
//        List<String> collectionsWithChunkAndVersion = findCollectionsWithChunkAndVersion(chunkedDeliveryKey, version);
//        for (String collection : collectionsWithChunkAndVersion) {
//            deleteChunkedVersionInCollectionWithoutRestart(collection, chunkedDeliveryKey, version);
//        }
        onDataChanged();
    }

    public void activateChunk(String chunkedDeliveryKey) {
        log.info("activateChunked (" + chunkedDeliveryKey + ")");
        // CARSTEN implement correctly
        // CARSTEN unit test
        onDataChanged();
    }


    public void inactivateChunk(String chunkedDeliveryKey) {
        log.info("inactivateChunk (" + chunkedDeliveryKey + ")");
        // CARSTEN implement correctly
        // CARSTEN unit test
        onDataChanged();
    }

    public void activateChunkedVersion(String chunkedDeliveryKey, String version) {
        log.info("activateChunkedVersion (" + chunkedDeliveryKey + ", " + version + ")");
        // CARSTEN implement correctly
//        List<String> matchingCollections = findCollectionsWithChunkAndVersion(chunkedDeliveryKey, version);
//        for (String matchingCollection : matchingCollections) {
//            activateChunkedVersionInCollectionWithoutRestart(matchingCollection, chunkedDeliveryKey, version);
//        }
        onDataChanged();
    }

//    private List<String> findCollectionsWithChunkAndVersion(String chunkedDeliveryKey, String version) {
//        List<File> collectionDirectories = findCollectionDataDirectories();
//        List<String> result = new LinkedList<String>();
//        for (File collectionDirectory : collectionDirectories) {
//            String path = collectionDirectory.getAbsolutePath() + "/" + chunkedDeliveryKey + "/" + version;
//            File pathFile = new File(path);
//            if (pathFile.exists()) {
//                result.add(collectionDirectory.getName());
//            }
//        }
//
//        return result;
//    }

//    private List<File> findCollectionDataDirectories() {
//        List<File> collectionDirectories = new LinkedList<File>();
//        File[] files = getDataPath().listFiles();
//        for (File file : files) {
//            if (!file.getName().startsWith(".") && file.isDirectory()) {
//                collectionDirectories.add(file);
//            }
//        }
//        return collectionDirectories;
//    }

    public String getActiveDeliveryVersion(String collection, String chunkDeliveryKey) {
        return ActiveProperties.getActiveDeliveryVersion(getActiveDeliveryFile(collection, chunkDeliveryKey));
    }

    private File getActiveDeliveryFile(String collection, String chunkDeliveryKey) {
        return new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + ActiveProperties.DEFAULT_FILENAME);
    }

//    private void activateDeliveryVersion(String version, File activeDeliveryFile) {
//        log.info("Activate " + activeDeliveryFile.getAbsolutePath() + " => " + version);
//        ActiveProperties.writeActiveFile(activeDeliveryFile, version);
//    }

    public File getIndexPath() {
        return config.getIndexPath();
    }

    public File getDataPath() {
        return config.getDataPath();
    }

    private void delete(File file) {
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
        List<ChunkedDeliveryVersion> chunkedDeliveryVersions = getChunkedDeliveryVersions(loadSizes);
        LinkedMultiValueMap<String, VersionedJumboCollection> versionedCollections = new LinkedMultiValueMap<String, VersionedJumboCollection>();
        Map<String, ChunkedDeliveryVersion> deliveries = new HashMap<String, ChunkedDeliveryVersion>();

        for (ChunkedDeliveryVersion chunkedDeliveryVersion : chunkedDeliveryVersions) {
            deliveries.put(chunkedDeliveryVersion.getChunkKey() + "/" + chunkedDeliveryVersion.getVersion(), chunkedDeliveryVersion);
            for (VersionedJumboCollection versionedJumboCollection : chunkedDeliveryVersion.getCollections()) {
                versionedCollections.add(versionedJumboCollection.getCollectionName(), versionedJumboCollection);
            }
        }

        List<JumboCollection> result = new LinkedList<JumboCollection>();
        for (String collection : versionedCollections.keySet()) {
            List<VersionedJumboCollection> versionedJumboCollections = versionedCollections.get(collection);
            LinkedMultiValueMap<String, VersionedJumboCollection> groupedByChunk = new LinkedMultiValueMap<String, VersionedJumboCollection>();
            for (VersionedJumboCollection versionedJumboCollection : versionedJumboCollections) {
                groupedByChunk.add(versionedJumboCollection.getChunkKey(), versionedJumboCollection);
            }
            List<DeliveryChunk> chunks = new LinkedList<DeliveryChunk>();
            for (String deliveryKey : groupedByChunk.keySet()) {
                List<VersionedJumboCollection> versions = groupedByChunk.get(deliveryKey);
                List<DeliveryVersion> chunkVersions = new LinkedList<DeliveryVersion>();
                boolean chunkActive = false;
                for (VersionedJumboCollection version : versions) {
                    ChunkedDeliveryVersion chunkedDeliveryVersion = deliveries.get(version.getChunkKey() + "/" + version.getVersion());
                    chunkVersions.add(new DeliveryVersion(version.getVersion(), chunkedDeliveryVersion.getInfo(), version.getDate(),
                            version.getCompressedSize(), version.getUncompressedSize(), version.getIndexSize(), chunkedDeliveryVersion.isVersionActive()));
                    chunkActive = chunkedDeliveryVersion.isChunkActive();
                }
                chunks.add(new DeliveryChunk(deliveryKey, chunkActive, chunkVersions));
            }
            result.add(new JumboCollection("col-" + collection.hashCode(), collection, chunks));
        }
        return result;
    }

//    private File getDeliveryPropertiesFile(File deliveryVersionFolder) {
//        return new File(deliveryVersionFolder.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME);
//    }

    public List<ChunkedDeliveryVersion> getChunkedDeliveryVersions() {
        return getChunkedDeliveryVersions(true);
    }

    public List<ChunkedDeliveryVersion> getChunkedDeliveryVersions(boolean loadSizes) {
        List<ChunkedDeliveryVersion> result = new LinkedList<ChunkedDeliveryVersion>();
        File[] deliveryKeys = getDataPath().listFiles(FOLDER_FILTER);
        for (File deliveryKeyPath : deliveryKeys) {
            String deliveryKey = deliveryKeyPath.getName();
            File[] versions = deliveryKeyPath.listFiles(FOLDER_FILTER);
            File activeProperties = new File(deliveryKeyPath.getAbsolutePath() + "/" + ActiveProperties.DEFAULT_FILENAME);
            boolean deliveryKeyIsActive = ActiveProperties.isDeliveryActive(activeProperties);
            String activeVersion = ActiveProperties.getActiveDeliveryVersion(activeProperties);
            for (File versionPath : versions) {
                String version = versionPath.getName();
                List<VersionedJumboCollection> collections = new ArrayList<VersionedJumboCollection>();
                for (File collectionPath : versionPath.listFiles(FOLDER_FILTER)) {
                    String collection = collectionPath.getName();
                    CollectionProperties.CollectionMeta deliveryMeta = CollectionProperties.getDeliveryMeta(new File(collectionPath.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME));
                    long compressedSize = loadSizes ? jumboSearcher.getDataCompressedSize(collection, deliveryKey, version) : 0l;
                    long uncompressedSize = loadSizes ? jumboSearcher.getDataUncompressedSize(collection, deliveryKey, version) : 0l;
                    long indexSize = loadSizes ? jumboSearcher.getIndexSize(collection, deliveryKey, version) : 0l;
                    collections.add(new VersionedJumboCollection(collection, version, deliveryKey, deliveryMeta.getDate(),
                            deliveryMeta.getSourcePath(), deliveryMeta.getStrategy(), compressedSize, uncompressedSize, indexSize));

                }
                String collapseId = "col" + (deliveryKey + "-" + version).hashCode();
                DeliveryProperties.DeliveryMeta deliveryMeta = DeliveryProperties.getDeliveryMeta(new File(versionPath.getAbsoluteFile() + "/" + DeliveryProperties.DEFAULT_FILENAME));
                String info = deliveryMeta.getInfo();
                String date = deliveryMeta.getDate();
                result.add(new ChunkedDeliveryVersion(collapseId, deliveryKey, version, info, date, activeVersion.equals(version), deliveryKeyIsActive, collections));
            }
        }
        Collections.sort(result);
        return result;
    }

//    private List<VersionedJumboCollection> getAllVersionedJumboCollections() {
//        List<VersionedJumboCollection> versionedJumboCollections = new LinkedList<VersionedJumboCollection>();
//        File[] collectionFolders = getDataPath().listFiles(FOLDER_FILTER);
//        for (File collectionFolder : collectionFolders) {
//            String collectionName = collectionFolder.getName();
//            File[] deliveryChunkFolders = collectionFolder.listFiles(FOLDER_FILTER);
//            for (File deliveryChunkFolder : deliveryChunkFolders) {
//                String chunkKey = deliveryChunkFolder.getName();
//                String activeVersion = getActiveDeliveryVersion(collectionName, chunkKey);
//                File[] versionFolders = deliveryChunkFolder.listFiles(FOLDER_FILTER);
//                for (File versionFolder : versionFolders) {
//                    String version = versionFolder.getName();
//                    CollectionProperties.CollectionMeta meta = CollectionProperties.getDeliveryMeta(getDeliveryPropertiesFile(versionFolder));
//                    boolean active = activeVersion.equals(version);
//                    long compressedSize = calculateCompressedSize(versionFolder);
//                    long uncompressedSize = getUncompressedSize(versionFolder);
//                    long indexSize = getIndexSize(collectionName, chunkKey, version);
//                    // CARSTEN replace me info
//                    versionedJumboCollections.add(new VersionedJumboCollection(collectionName, version, chunkKey, "replace me info", dateToString(meta.getDate()), meta.getSourcePath(), meta.getStrategy(), active, compressedSize, uncompressedSize, indexSize));
//                }
//            }
//
//        }
//        return versionedJumboCollections;
//        return null;
//    }

//    public List<MetaData> getMetaDataForDelivery(String deliveryChunkKey, String version, boolean activate) {
//        List<VersionedJumboCollection> allVersionedJumboCollections = getAllVersionedJumboCollections();
//        List<MetaData> result = new LinkedList<MetaData>();
//        for (VersionedJumboCollection collection : allVersionedJumboCollections) {
//            if (deliveryChunkKey.equals(collection.getChunkKey()) && version.equals(collection.getVersion())) {
//                result.add(new MetaData(collection.getCollectionName(), deliveryChunkKey, version, collection.getStrategy(), collection.getSourcePath(), activate, collection.getInfo()));
//            }
//        }
//        return result;
//    }
//
//    public List<MetaIndex> getMetaIndexForDelivery(String deliveryChunkKey, String version) {
//        List<VersionedJumboCollection> allVersionedJumboCollections = getAllVersionedJumboCollections();
//        List<MetaIndex> result = new LinkedList<MetaIndex>();
//        for (VersionedJumboCollection collection : allVersionedJumboCollections) {
//            if (deliveryChunkKey.equals(collection.getChunkKey()) && version.equals(collection.getVersion())) {
//                List<CollectionIndex> collectionIndexes = getCollectionIndexes(collection.getCollectionName(), deliveryChunkKey, version);
//                for (CollectionIndex collectionIndex : collectionIndexes) {
//                    result.add(new MetaIndex(collection.getCollectionName(), deliveryChunkKey, version, collectionIndex.getIndexName(), collectionIndex.getStrategy(), collectionIndex.getIndexSourceFields()));
//                }
//            }
//        }
//        Collections.sort(result);
//        return result;
//    }

    private List<CollectionIndex> getCollectionIndexes(String collectionName, String deliveryChunkKey, String version) {
        File collectionVersionIndexPath = findCollectionChunkedVersionIndexFolder(collectionName, deliveryChunkKey, version);
        File[] indexFolders = collectionVersionIndexPath.listFiles(FOLDER_FILTER);
        if (indexFolders == null) {
            return Collections.emptyList();
        }
        List<CollectionIndex> result = new LinkedList<CollectionIndex>();
        for (File indexFolder : indexFolders) {
            IndexProperties.IndexMeta props = IndexProperties.getIndexMeta(new File(indexFolder.getAbsolutePath() + "/" + IndexProperties.DEFAULT_FILENAME));
            result.add(new CollectionIndex(indexFolder.getName(), props.getDate(), props.getIndexSourceFields(), props.getStrategy()));
        }
        return result;
    }

    private File findCollectionChunkedVersionIndexFolder(String collectionName, String deliveryChunkKey, String version) {
        return new File(getIndexPath().getAbsolutePath() + "/" + deliveryChunkKey + "/" + version + "/" + collectionName + "/");
    }
//
//    private File findCollectionChunkedVersionIndexFolder(String collectionName, String deliveryChunkKey, String version, String indexName) {
//        return new File(getIndexPath().getAbsolutePath() + "/" + collectionName + "/" + deliveryChunkKey + "/" + version + "/" + indexName + "/");
//    }
//
//    private File findCollectionChunkedVersionDataFolder(String collectionName, String deliveryChunkKey, String version) {
//        return new File(getDataPath().getAbsolutePath() + "/" + collectionName + "/" + deliveryChunkKey + "/" + version + "/");
//    }

//    public List<IndexInfo> getIndexInfoForDelivery(List<MetaIndex> metaIndex) {
//        List<IndexInfo> result = new LinkedList<IndexInfo>();
//        for (MetaIndex index : metaIndex) {
//            // CARSTEN fix me implementation details of strategy
//            File indexFolder = findCollectionChunkedVersionIndexFolder(index.getCollection(), index.getDeliveryKey(), index.getDeliveryVersion(), index.getIndexName());
//            FilenameFilter ioFileFilter = FileFilterUtils.suffixFileFilter(".idx");
//            File[] files = indexFolder.listFiles(ioFileFilter);
//            for (File indexFile : files) {
//                long fileLength = getSizeFromSnappyChunk(new File(indexFile.getAbsolutePath() + ".chunks"));
//                result.add(new IndexInfo(index.getCollection(), index.getIndexName(), indexFile.getName(), fileLength, index.getDeliveryKey(), index.getDeliveryVersion(), index.getIndexStrategy()));
//            }
//        }
//        Collections.sort(result);
//        return result;
//    }

//    public List<DataInfo> getDataInfoForDelivery(List<MetaData> metaDatas) {
//        List<DataInfo> result = new LinkedList<DataInfo>();
//        IOFileFilter notPointPrefix = FileFilterUtils.notFileFilter(FileFilterUtils.prefixFileFilter("."));
//        IOFileFilter notUnderScore = FileFilterUtils.notFileFilter(FileFilterUtils.prefixFileFilter("_"));
//        IOFileFilter notChunksSnappy = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".chunks"));
//        IOFileFilter notProperties = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".properties"));
//        IOFileFilter notSha1 = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".sha1"));
//        IOFileFilter notMd5 = FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".md5"));
//        FilenameFilter ioFileFilter = FileFilterUtils.and(notChunksSnappy, notProperties, notSha1, notMd5, notPointPrefix, notUnderScore);
//        for (MetaData data : metaDatas) {
//            File dataFolder = findCollectionChunkedVersionDataFolder(data.getCollection(), data.getDeliveryKey(), data.getDeliveryVersion());
//            File[] files = dataFolder.listFiles(ioFileFilter);
//            for (File dataFile : files) {
//                // CARSTEN fix me implementation details of strategy
//                long fileLength = getSizeFromSnappyChunk(new File(dataFile.getAbsolutePath() + ".chunks"));
//                result.add(new DataInfo(data.getCollection(), dataFile.getName(), fileLength, data.getDeliveryKey(), data.getDeliveryVersion(), data.getDataStrategy()));
//            }
//        }
//        return result;
//    }
//
//    public InputStream getInputStream(IndexInfo index) throws IOException {
//        File indexFolder = findCollectionChunkedVersionIndexFolder(index.getCollection(), index.getDeliveryKey(), index.getDeliveryVersion(), index.getIndexName());
//        File indexFile = new File(indexFolder.getAbsolutePath() + "/" + index.getFilename());
//        return new SnappyInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
//    }
//
//    public InputStream getInputStream(DataInfo data) throws IOException {
//        File dataFolder = findCollectionChunkedVersionDataFolder(data.getCollection(), data.getDeliveryKey(), data.getDeliveryVersion());
//        File dataFile = new File(dataFolder.getAbsolutePath() + "/" + data.getFilename());
//        return new SnappyInputStream(new BufferedInputStream(new FileInputStream(dataFile)));
//    }
//
//    private static class ChunkKeyVersion {
//        final String chunkKey;
//        final String version;
//
//        private ChunkKeyVersion(String chunkKey, String version) {
//            this.chunkKey = chunkKey;
//            this.version = version;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//
//            ChunkKeyVersion that = (ChunkKeyVersion) o;
//
//            if (!chunkKey.equals(that.chunkKey)) return false;
//            if (!version.equals(that.version)) return false;
//
//            return true;
//        }
//
//        @Override
//        public int hashCode() {
//            int result = chunkKey.hashCode();
//            result = 31 * result + version.hashCode();
//            return result;
//        }
//    }
}
