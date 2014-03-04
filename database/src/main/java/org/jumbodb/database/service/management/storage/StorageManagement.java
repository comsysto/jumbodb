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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
// CARSTEN fix unit tests
// CARSTEN change all signatures to chunk key, version, collection name
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
        // CARSTEN unit test
        log.info("deleteChunkedVersion (" + chunkedDeliveryKey + ", " + version + ")");
        try {
            File[] versionFolders = getDataChunkFolder(chunkedDeliveryKey).listFiles(FOLDER_FILTER);
            if(versionFolders.length == 1) {
                FileUtils.deleteDirectory(getDataChunkFolder(chunkedDeliveryKey));
                FileUtils.deleteDirectory(getIndexChunkFolder(chunkedDeliveryKey));
            }
            else {
                FileUtils.deleteDirectory(getDataChunkedVersionFolder(chunkedDeliveryKey, version));
                FileUtils.deleteDirectory(getIndexChunkedVersionFolder(chunkedDeliveryKey, version));
                activateChunkedLatestVersion(chunkedDeliveryKey);
            }
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
        onDataChanged();
    }

    private File getIndexChunkFolder(String chunkedDeliveryKey) {
        return new File(config.getIndexPath().getAbsolutePath() + "/" + chunkedDeliveryKey + "/");
    }

    private File getIndexChunkedVersionFolder(String chunkedDeliveryKey, String version) {
        return new File(getIndexChunkFolder(chunkedDeliveryKey).getAbsolutePath() + version + "/");
    }

    private File getDataChunkFolder(String chunkedDeliveryKey) {
        return new File(config.getDataPath().getAbsolutePath() + "/" + chunkedDeliveryKey + "/");
    }

    private File getDataChunkedVersionFolder(String chunkedDeliveryKey, String version) {
        return new File(getDataChunkFolder(chunkedDeliveryKey).getAbsolutePath() + version + "/");
    }

    private void activateChunkedLatestVersion(String chunkedDeliveryKey) {
        String latestVersion = getLatestVersionInChunk(chunkedDeliveryKey);
        activateChunkedVersion(chunkedDeliveryKey, latestVersion);
    }

    private String getLatestVersionInChunk(String chunkedDeliveryKey) {
        File[] versionFolders = getDataChunkFolder(chunkedDeliveryKey).listFiles(FOLDER_FILTER);
        if(versionFolders == null || versionFolders.length == 0l) {
            throw new IllegalStateException("No available version in chunk folder! Chunk folder " + chunkedDeliveryKey + " must be deleted!");
        }
        try {

            String version = "illegal version";
            Date date = new Date(0l);
            SimpleDateFormat sdf = new SimpleDateFormat(DeliveryProperties.DATE_PATTERN);
            for (File versionFolder : versionFolders) {
                File deliveryProp = getDeliveryFile(versionFolder);
                DeliveryProperties.DeliveryMeta deliveryMeta = DeliveryProperties.getDeliveryMeta(deliveryProp);
                Date parse = sdf.parse(deliveryMeta.getDate());
                if(parse.after(date)) {
                    date = parse;
                    version = versionFolder.getName();
                }
            }
            return version;
        } catch (ParseException e) {
            log.error("Could not parse date of delivery version", e);
            return versionFolders[0].getName();
        }
    }

    private File getDeliveryFile(File versionFolder) {
        return new File(versionFolder.getAbsolutePath() + "/" + DeliveryProperties.DEFAULT_FILENAME);
    }

    private File getActiveFile(File chunkFolder) {
        return new File(chunkFolder.getAbsolutePath() + "/" + ActiveProperties.DEFAULT_FILENAME);
    }

    public void activateChunk(String chunkedDeliveryKey) {
        // CARSTEN unit test
        log.info("activateChunk(" + chunkedDeliveryKey + ")");
        changeChunkActiveState(chunkedDeliveryKey, true);
        onDataChanged();
    }

    private void changeChunkActiveState(String chunkedDeliveryKey, boolean chunkActiveState) {
        File dataChunkFolder = getDataChunkFolder(chunkedDeliveryKey);
        File activeFile = getActiveFile(dataChunkFolder);
        String activeDeliveryVersion = ActiveProperties.getActiveDeliveryVersion(activeFile);
        ActiveProperties.writeActiveFile(activeFile, activeDeliveryVersion, chunkActiveState);
    }


    public void inactivateChunk(String chunkedDeliveryKey) {
        // CARSTEN unit test
        log.info("inactivateChunk(" + chunkedDeliveryKey + ")");
        changeChunkActiveState(chunkedDeliveryKey, false);
        onDataChanged();
    }

    public void activateChunkedVersion(String chunkedDeliveryKey, String version) {
        // CARSTEN unit test
        log.info("activateChunkedVersion(" + chunkedDeliveryKey + ", " + version + ")");
        File dataChunkFolder = getDataChunkFolder(chunkedDeliveryKey);
        File activeFile = getActiveFile(dataChunkFolder);
        boolean chunkActive = ActiveProperties.isDeliveryActive(activeFile);
        ActiveProperties.writeActiveFile(activeFile, chunkedDeliveryKey, chunkActive);
        onDataChanged();
    }

    public String getActiveDeliveryVersion(String collection, String chunkDeliveryKey) {
        return ActiveProperties.getActiveDeliveryVersion(getActiveDeliveryFile(collection, chunkDeliveryKey));
    }

    private File getActiveDeliveryFile(String collection, String chunkDeliveryKey) {
        return new File(getDataPath().getAbsolutePath() + "/" + collection + "/" + chunkDeliveryKey + "/" + ActiveProperties.DEFAULT_FILENAME);
    }

    public File getIndexPath() {
        return config.getIndexPath();
    }

    public File getDataPath() {
        return config.getDataPath();
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
                    CollectionProperties.CollectionMeta deliveryMeta = CollectionProperties.getCollectionMeta(
                      new File(collectionPath.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME));
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

//    public List<IndexInfo> getIndexInfoForDelivery(List<MetaIndex> metaIndex) {
//        List<IndexInfo> result = new LinkedList<IndexInfo>();
//        for (MetaIndex index : metaIndex) {
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
}
