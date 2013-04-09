package org.jumbodb.database.service.importer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.jumbodb.database.service.query.Restartable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ImportTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(ImportTask.class);

    public static final int SNAPPY_DATA_CHUNK_SIZE = 32 * 1024;
    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte data hash, 4 byte file name hash, 8 byte offset)
    private Socket clientSocket;
    public static final String STORAGE_VERSION = "1";
    private int clientID;
    private File dataPath;
    private File indexPath;
    private Restartable queryServer;

    public ImportTask(Socket s, int i, File dataPath, File indexPath, Restartable queryServer) {
        this.clientSocket = s;
        this.clientID = i;
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.queryServer = queryServer;
    }

    public void run() {
        log.info("ImportServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress().getHostName());
        DatabaseImportSession databaseImportSession = null;
        try {
            databaseImportSession = new DatabaseImportSession(clientSocket, clientID);
            databaseImportSession.runImport(new ImportHandler() {
                @Override
                public void onImport(ImportMetaFileInformation information, InputStream dataInputStream) {
                    OutputStream sos = null;
                    DataOutputStream dos = null;
                    BufferedOutputStream bos = null;
                    FileOutputStream snappyChunksFos = null;
                    DataOutputStream snappyChunksDos = null;
                    try {
                        String absoluteImportPath = getTemporaryImportAbsolutePathByType(information);
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
                        int chunkSize = information.getFileType() == ImportMetaFileInformation.FileType.DATA ? SNAPPY_DATA_CHUNK_SIZE : SNAPPY_INDEX_CHUNK_SIZE;
                        snappyChunksFos = new FileOutputStream(filePlaceChunksFile);
                        snappyChunksDos = new DataOutputStream(snappyChunksFos);
                        final DataOutputStream finalSnappyChunksDos = snappyChunksDos;

                        snappyChunksDos.writeLong(information.getFileLength());
                        snappyChunksDos.writeInt(chunkSize);
                        // CARSTEN pfui, cleanup when time!
                        bos = new BufferedOutputStream(new FileOutputStream(filePlacePathFile)) {
                            @Override
                            public synchronized void write(byte[] bytes, int i, int i2) throws IOException {
                                finalSnappyChunksDos.writeInt(i2);
                                super.write(bytes, i, i2);
                            }
                        };
                        sos = new SnappyOutputStream(bos, chunkSize);
//                        } else {
//                            sos = new FileOutputStream(filePlacePathFile);
//                        }
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
                        IOUtils.closeQuietly(clientSocket);
                    }
                }

                @Override
                public void onCollectionMetaInformation(ImportMetaInformation information) {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                    String deliveryKeyPath = getTemporaryDataPath(information.getDeliveryKey(), information.getDeliveryVersion())+ "/" + information.getCollection() + "/";
                    Properties deliveryInfo = new Properties();
                    deliveryInfo.setProperty("deliveryVersion", information.getDeliveryVersion());
                    deliveryInfo.setProperty("sourcePath", information.getSourcePath());
                    deliveryInfo.setProperty("date", sdf.format(new Date()));
                    deliveryInfo.setProperty("info", information.getInfo());
                    deliveryInfo.setProperty("storageVersion", STORAGE_VERSION);


                    File deliveryVersionFilePath = new File(deliveryKeyPath);
                    if(!deliveryVersionFilePath.exists()) {
                        deliveryVersionFilePath.mkdirs();
                    }
                    File deliveryInfoFile = new File(deliveryKeyPath + "/delivery.properties");
                    FileOutputStream deliveryInfoFos = null;
                    try {
                        deliveryInfoFos = new FileOutputStream(deliveryInfoFile);
                        deliveryInfo.store(deliveryInfoFos, "Delivery Information");
                    } catch(IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        IOUtils.closeQuietly(deliveryInfoFos);
                    }
                    // pfad sollte der richtige sein ...
                    File activeDeliveryFile = getFinalActivationFilePath(information.getCollection(), information.getDeliveryKey());//new File(deliveryKeyPath + "/active.properties");
                    if(!activeDeliveryFile.exists()) {
                        onActivateDelivery(information);
                    }
                }

                @Override
                public void onActivateDelivery(ImportMetaInformation information) {
                    File activationPath = getTemporaryActivationPath(dataPath, information.getDeliveryKey(), information.getDeliveryVersion());
                    if(!activationPath.exists()) {
                        activationPath.mkdirs();
                    }
                    File activeDeliveryFile = new File(activationPath.getAbsoluteFile() + "/" + information.getCollection());
                    ImportHelper.writeActiveFile(activeDeliveryFile, information.getDeliveryVersion());
                }

                @Override
                public void onFinished(String deliveryKey, String deliveryVersion) {
                    log.info("Moving temporary data to final path");
                    moveDataFiles(deliveryKey, deliveryVersion);
                    log.info("Temporary data to final path moved");
                    log.info("Moving temporary index to final path");
                    moveIndexFiles(deliveryKey, deliveryVersion);
                    log.info("Temporary index to final path moved");
                    log.info("Moving activation files to final path");
                    moveActivationFiles(deliveryKey, deliveryVersion);
                    log.info("Activation files to final path moved");

                    log.info("Cleaning up temporary stuff");
                    try {
                        FileUtils.deleteDirectory(getTemporaryDeliveryPath(dataPath, deliveryKey, deliveryVersion));
                        FileUtils.deleteDirectory(getTemporaryDeliveryPath(indexPath, deliveryKey, deliveryVersion));
                    } catch (IOException e) {
                        log.error(e.toString());
                        throw new RuntimeException(e);
                    }
                    log.info("Temporary stuff cleaned up");
                    log.info("Restarting Query Server");
                    queryServer.restart();
                    log.info("Restarted Query Server");
                }

                private void moveActivationFiles(String deliveryKey, String deliveryVersion) {
                    File temporaryActivationPath = getTemporaryActivationPath(dataPath, deliveryKey, deliveryVersion);
                    if(!temporaryActivationPath.exists()) {
                        // nothing to activate
                        return;
                    }
                    File[] collectionFiles = temporaryActivationPath.listFiles();
                    for (File collectionFile : collectionFiles) {
                        File finalFile = getFinalActivationFilePath(collectionFile.getName(), deliveryKey);
                        if(finalFile.exists()) {
                            finalFile.delete();
                        }
                        collectionFile.renameTo(finalFile);
                    }
                }

                private void moveIndexFiles(String deliveryKey, String deliveryVersion) {
                    File temporaryIndexPath = getTemporaryIndexPath(deliveryKey, deliveryVersion);
                    FileFilter directory = DirectoryFileFilter.INSTANCE;
                    File[] collectionFolders = temporaryIndexPath.listFiles(directory);
                    if(collectionFolders != null) {
                        for (File collectionFolder : collectionFolders) {
                            File[] indexFolders = collectionFolder.listFiles(directory);
                            for (File indexFolder : indexFolders) {
                                File finalFolder = getFinalIndexPath(collectionFolder.getName(), indexFolder.getName(), deliveryKey, deliveryVersion);
                                if(!finalFolder.getParentFile().exists()) {
                                    finalFolder.getParentFile().mkdirs();
                                }
                                indexFolder.renameTo(finalFolder);
                            }
                        }
                    }
                }

                private void moveDataFiles(String deliveryKey, String deliveryVersion) {
                    File temporaryDataPath = getTemporaryDataPath(deliveryKey, deliveryVersion);
                    File[] collectionFolders = temporaryDataPath.listFiles((FileFilter) DirectoryFileFilter.INSTANCE);
                    for (File collectionFolder : collectionFolders) {
                        File finalFolder = getFinalDataPath(collectionFolder.getName(), deliveryKey, deliveryVersion);
                        if(!finalFolder.getParentFile().exists()) {
                            finalFolder.getParentFile().mkdirs();
                        }
                        collectionFolder.renameTo(finalFolder);
                    }
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(databaseImportSession);
        }
    }

    private File getFinalIndexPath(String collection, String indexName, String deliveryKey, String deliveryVersion) {
        return new File(indexPath.getAbsolutePath() + "/" + collection + "/" + deliveryKey + "/" + deliveryVersion + "/" + indexName + "/");
    }


    private File getFinalActivationFilePath(String collection, String deliveryKey) {
        return new File(dataPath.getAbsolutePath() + "/" + collection + "/" + deliveryKey + "/active.properties");
    }

    private File getFinalDataPath(String collection, String deliveryKey, String deliverVersion) {
        return new File(dataPath.getAbsolutePath() + "/" + collection + "/" + deliveryKey + "/" + deliverVersion + "/");
    }

    private File getTemporaryDataPath(String deliveryKey, String deliverVersion) {
        return getTemporaryImportPath(dataPath, deliveryKey, deliverVersion);
    }

    private File getTemporaryIndexPath(String deliveryKey, String deliverVersion) {
        return getTemporaryImportPath(indexPath, deliveryKey, deliverVersion);
    }

    private File getTemporaryPath(File path) {
        return new File(path.getAbsolutePath() + "/.tmp/");
    }

    private File getTemporaryImportPath(File path, String deliveryKey, String deliveryVersion) {
        File temporaryDeliveryPath = getTemporaryDeliveryPath(path, deliveryKey, deliveryVersion);
        return new File(temporaryDeliveryPath.getAbsolutePath() + "/import");
    }

    private File getTemporaryDeliveryPath(File path, String deliveryKey, String deliveryVersion) {
        return new File(getTemporaryPath(path).getAbsolutePath() + "/" + deliveryKey + "_" + deliveryVersion);
    }

    private File getTemporaryActivationPath(File path, String deliveryKey, String deliveryVersion) {
        File temporaryDeliveryPath = getTemporaryDeliveryPath(path, deliveryKey, deliveryVersion);
        return new File(temporaryDeliveryPath.getAbsolutePath() + "/activate");
    }


    private String getTemporaryImportAbsolutePathByType(ImportMetaFileInformation information) throws IOException {
        if (information.getFileType() == ImportMetaFileInformation.FileType.DATA) {
            return getTemporaryDataPath(information.getDeliveryKey(), information.getDeliveryVersion()) + "/" + information.getCollection() + "/";
        } else if (information.getFileType() == ImportMetaFileInformation.FileType.INDEX) {
            return getTemporaryIndexPath(information.getDeliveryKey(), information.getDeliveryVersion()) + "/" + information.getCollection() + "/" + information.getIndexName() + "/";
        }
        throw new IllegalArgumentException("Type " + information.getFileType() + " is not allowed, only data and index.");
    }

//    private String getImportAbsolutePathByType(String deliveryKey, String deliveryVersion, ) throws IOException {
//        if (information.getFileType() == ImportMetaFileInformation.FileType.DATA) {
//            return dataPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information.getDeliveryKey() + "/" + information.getDeliveryVersion() + "/";
//        } else if (information.getFileType() == ImportMetaFileInformation.FileType.INDEX) {
//            return indexPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information.getDeliveryKey() + "/" + information.getDeliveryVersion() + "/" + information.getIndexName() + "/";
//        }
//        throw new IllegalArgumentException("Type " + information.getFileType() + " is not allowed, only data and index.");
//    }
}