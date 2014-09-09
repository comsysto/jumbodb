package org.jumbodb.database.service.management.storage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.ChecksumType;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.connector.importer.DataInfo;
import org.jumbodb.connector.importer.IndexInfo;
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

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * User: carsten
 * Date: 3/22/13
 * Time: 2:12 PM
 */
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
            if(collection.hasAtLeastOneActiveChunk()) {
                Set<QueryUtilIndex> resultIndexes = new HashSet<QueryUtilIndex>();
                for (DeliveryChunk deliveryChunk : collection.getChunks()) {
                    for (DeliveryVersion deliveryVersion : deliveryChunk.getVersions()) {
                        if(deliveryVersion.isActive() && deliveryChunk.isActive()) {
                            List<CollectionIndex> collectionIndexes = getCollectionIndexes(deliveryChunk.getKey(), deliveryVersion.getVersion(), collection.getName());
                            for (CollectionIndex collectionIndex : collectionIndexes) {
                                IndexStrategy indexStrategy = jumboSearcher.getIndexStrategy(collectionIndex.getStrategy());
                                resultIndexes.add(new QueryUtilIndex(collectionIndex.getIndexName(), collectionIndex.getStrategy(), new ArrayList<QueryOperation>(indexStrategy.getSupportedOperations())));
                            }
                            DataStrategy dataStrategy = jumboSearcher.getDataStrategy(deliveryChunk.getKey(),
                              collection.getName());
                            dataStrategyName = dataStrategy.getStrategyName();
                            supportedOperations.addAll(dataStrategy.getSupportedOperations());
                        }
                    }
                }
                List<QueryUtilIndex> indexes = new ArrayList<QueryUtilIndex>(resultIndexes);
                Collections.sort(indexes);
                result.add(new QueryUtilCollection(collection.getName(), indexes, dataStrategyName, new ArrayList<QueryOperation>(supportedOperations)));
            }
        }
        return result;
    }

    public void deleteChunkedVersion(String chunkedDeliveryKey, String version) {
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
        return new File(getIndexChunkFolder(chunkedDeliveryKey).getAbsolutePath() + "/" + version + "/");
    }

    private File getDataChunkFolder(String chunkedDeliveryKey) {
        return new File(config.getDataPath().getAbsolutePath() + "/" + chunkedDeliveryKey + "/");
    }

    private File getDataChunkedVersionFolder(String chunkedDeliveryKey, String version) {
        return new File(getDataChunkFolder(chunkedDeliveryKey).getAbsolutePath() + "/" + version + "/");
    }

    private File getDataChunkedVersionCollectionFolder(String chunkedDeliveryKey, String version, String collection) {
        return new File(getDataChunkedVersionFolder(chunkedDeliveryKey, version).getAbsolutePath() + "/" + collection + "/");
    }

    private void activateChunkedLatestVersion(String chunkedDeliveryKey) {
        String latestVersion = getLatestVersionInChunk(chunkedDeliveryKey);
        activateChunkedVersion(chunkedDeliveryKey, latestVersion);
    }

    protected String getLatestVersionInChunk(String chunkedDeliveryKey) {
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
        log.info("inactivateChunk(" + chunkedDeliveryKey + ")");
        changeChunkActiveState(chunkedDeliveryKey, false);
        onDataChanged();
    }

    public void activateChunkedVersion(String chunkedDeliveryKey, String version) {
        log.info("activateChunkedVersion(" + chunkedDeliveryKey + ", " + version + ")");
        File dataChunkFolder = getDataChunkFolder(chunkedDeliveryKey);
        File activeFile = getActiveFile(dataChunkFolder);
        boolean chunkActive = ActiveProperties.isDeliveryActive(activeFile);
        ActiveProperties.writeActiveFile(activeFile, version, chunkActive);
        onDataChanged();
    }

    public String getActiveDeliveryVersion(String chunkDeliveryKey) {
        return ActiveProperties.getActiveDeliveryVersion(getActiveDeliveryFile(chunkDeliveryKey));
    }

    private File getActiveDeliveryFile(String chunkDeliveryKey) {
        return new File(getDataPath().getAbsolutePath() + "/" + chunkDeliveryKey + "/" + ActiveProperties.DEFAULT_FILENAME);
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
                            version.getCompressedSize(), version.getUncompressedSize(), version.getIndexSize(), chunkedDeliveryVersion.isActiveVersion()));
                    chunkActive = chunkedDeliveryVersion.isActiveChunk();
                }
                chunks.add(new DeliveryChunk(deliveryKey, chunkActive, chunkVersions));
            }
            result.add(new JumboCollection("col-" + collection.hashCode(), collection, getInfos(
              versionedJumboCollections), chunks));
        }
        return result;
    }

    private List<String> getInfos(final List<VersionedJumboCollection> versionedJumboCollections) {
        Set<String> result = new HashSet<String>();
        for (VersionedJumboCollection collection : versionedJumboCollections) {
            result.add(collection.getInfo());
        }
        return new ArrayList<String>(result);
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
                    CollectionProperties.CollectionMeta collectionMeta = CollectionProperties.getCollectionMeta(
                      new File(collectionPath.getAbsolutePath() + "/" + CollectionProperties.DEFAULT_FILENAME));
                    long compressedSize = loadSizes ? jumboSearcher.getDataCompressedSize(deliveryKey, version,
                      collection) : 0l;
                    long uncompressedSize = loadSizes ? jumboSearcher.getDataUncompressedSize(deliveryKey, version,
                      collection) : 0l;
                    long indexSize = loadSizes ? jumboSearcher.getIndexSize(deliveryKey, version, collection) : 0l;
                    collections.add(new VersionedJumboCollection(deliveryKey, version, collection, collectionMeta.getInfo(), collectionMeta.getDate(),
                            collectionMeta.getSourcePath(), collectionMeta.getStrategy(), compressedSize, uncompressedSize, indexSize));

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

    private List<CollectionIndex> getCollectionIndexes(String deliveryChunkKey, String version, String collectionName) {
        File collectionVersionIndexPath = getIndexChunkedVersionCollectionFolder(deliveryChunkKey, version, collectionName
        );
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


    private File getIndexChunkedVersionCollectionIndexFolder(String deliveryChunkKey, String version, String collectionName, String index) {
        return new File(getIndexPath().getAbsolutePath() + "/" + deliveryChunkKey + "/" + version + "/" + collectionName + "/" + index + "/");
    }

    private File getIndexChunkedVersionCollectionFolder(String deliveryChunkKey, String version, String collectionName) {
        return new File(getIndexPath().getAbsolutePath() + "/" + deliveryChunkKey + "/" + version + "/" + collectionName + "/");
    }

    public List<IndexInfo> getIndexInfoForDelivery(String deliveryChunkKey, String deliveryVersion) {
        ChunkedDeliveryVersion chunkedDeliveryVersion = getChunkedDeliveryVersion(deliveryChunkKey, deliveryVersion);
        List<IndexInfo> result = new LinkedList<IndexInfo>();
        for (VersionedJumboCollection versionedJumboCollection : chunkedDeliveryVersion.getCollections()) {
            String collectionName = versionedJumboCollection.getCollectionName();
            List<CollectionIndex> collectionIndexes = getCollectionIndexes(deliveryChunkKey, deliveryVersion, collectionName);
            for (CollectionIndex collectionIndex : collectionIndexes) {
                String indexName = collectionIndex.getIndexName();
                File indexFolder = getIndexChunkedVersionCollectionIndexFolder(deliveryChunkKey, deliveryVersion, collectionName, indexName);
                File[] files = indexFolder.listFiles();
                if(files == null) {
                    throw new IllegalStateException("Index folder does not exist for export: " + indexFolder.getAbsolutePath());
                }
                for (File indexFile : files) {
                    long fileLength = indexFile.length();
                    ChecksumType checksumType = resolveChecksumType(indexFile);
                    String checksum = resolveChecksum(checksumType, indexFile);
                    result.add(new IndexInfo(deliveryChunkKey, deliveryVersion, collectionName, indexName, indexFile.getName(), fileLength, checksumType, checksum));
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    private String resolveChecksum(ChecksumType checksumType, File indexFile) {
        if(checksumType == ChecksumType.NONE) {
            return null;
        }
        File checksumFile = new File(indexFile.getAbsolutePath() + checksumType.getFileSuffix());
        try {
            return FileUtils.readFileToString(checksumFile);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private ChecksumType resolveChecksumType(File file) {
        if(file.getName().startsWith(".") || file.getName().startsWith("_")) {
            return ChecksumType.NONE;
        }
        ChecksumType[] values = ChecksumType.values();
        for (ChecksumType checksum : values) {
            File checksumFile = new File(file.getAbsolutePath() + checksum.getFileSuffix());
            if(checksumFile.exists()) {
                return checksum;
            }
        }
        return ChecksumType.NONE;
    }

    public ChunkedDeliveryVersion getChunkedDeliveryVersion(String deliveryChunkKey, String deliveryVersion) {
        // could be more efficient, dont build full tree just the element itself
        List<ChunkedDeliveryVersion> chunkedDeliveryVersions = getChunkedDeliveryVersions(false);
        for (ChunkedDeliveryVersion chunkedDeliveryVersion : chunkedDeliveryVersions) {
            if(chunkedDeliveryVersion.getChunkKey().equals(deliveryChunkKey) && chunkedDeliveryVersion.getVersion().equals(deliveryVersion)) {
                return chunkedDeliveryVersion;
            }
        }
        return null;
    }

    public List<DataInfo> getDataInfoForDelivery(String deliveryChunkKey, String deliveryVersion) {
        List<DataInfo> result = new LinkedList<DataInfo>();
        ChunkedDeliveryVersion chunkedDeliveryVersion = getChunkedDeliveryVersion(deliveryChunkKey, deliveryVersion);
        for (VersionedJumboCollection versionedJumboCollection : chunkedDeliveryVersion.getCollections()) {
            String collectionName = versionedJumboCollection.getCollectionName();
            File dataFolder = getDataChunkedVersionCollectionFolder(deliveryChunkKey, deliveryVersion, collectionName);
            File[] files = dataFolder.listFiles();
            if(files == null) {
                throw new IllegalStateException("Data folder does not exist for export: " + dataFolder.getAbsolutePath());
            }
            for (File dataFile : files) {
                long fileLength = dataFile.length();
                ChecksumType checksumType = resolveChecksumType(dataFile);
                String checksum = resolveChecksum(checksumType, dataFile);
                result.add(new DataInfo(deliveryChunkKey, deliveryVersion, collectionName, dataFile.getName(), fileLength, checksumType, checksum));
            }
        }
        return result;
    }

    public InputStream getInputStream(IndexInfo index) throws IOException {
        File indexFolder = getIndexChunkedVersionCollectionFolder(index.getDeliveryKey(), index.getDeliveryVersion(), index.getCollection()
        );
        File indexFile = new File(indexFolder.getAbsolutePath() + "/" + index.getIndexName() + "/" + index.getFileName());
        return new BufferedInputStream(new FileInputStream(indexFile));
    }

    public InputStream getInputStream(DataInfo data) throws IOException {
        File dataFolder = getDataChunkedVersionCollectionFolder(data.getDeliveryKey(),
          data.getDeliveryVersion(), data.getCollection());
        File dataFile = new File(dataFolder.getAbsolutePath() + "/" + data.getFileName());
        return new BufferedInputStream(new FileInputStream(dataFile));
    }

}
