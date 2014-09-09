package org.jumbodb.database.service.importer;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.data.common.meta.ActiveProperties;
import org.jumbodb.common.query.ChecksumType;
import org.jumbodb.data.common.meta.DeliveryProperties;
import org.jumbodb.database.service.management.storage.StorageManagement;
import org.jumbodb.database.service.query.JumboSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ImportTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(ImportTask.class);

    private Socket clientSocket;
    private int clientID;
    private File dataPath;
    private File indexPath;
    private JumboSearcher jumboSearcher;

    public ImportTask(Socket s, int i, File dataPath, File indexPath, JumboSearcher jumboSearcher) {
        this.clientSocket = s;
        this.clientID = i;
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.jumboSearcher = jumboSearcher;
    }

    public void run() {
        log.info("ImportServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress()
          .getHostName());
        DatabaseImportSession databaseImportSession = null;
        try {
            databaseImportSession = createDatabaseImportSession();
            databaseImportSession.runImport(new ImportHandler() {
                @Override
                public void onInit(String deliveryKey, String deliveryVersion, String date,
                  String info) throws DeliveryVersionExistsException {
                    if (existsDeliveryVersion(deliveryKey, deliveryVersion)) {
                        throw new DeliveryVersionExistsException("Delivery version '" + deliveryVersion
                          + "' for chunk '" + deliveryKey + "' already exists!");
                    }
                    File tmpIndexImportPath = getTmpIndexDeliveryPath(deliveryKey, deliveryVersion);
                    deleteFolderIfExists(tmpIndexImportPath);
                    File tmpDataImportPath = getTmpDataDeliveryPath(deliveryKey, deliveryVersion);
                    deleteFolderIfExists(tmpDataImportPath);
                    tmpIndexImportPath.mkdirs();
                    tmpDataImportPath.mkdirs();
                    File deliveryFile = new File(
                      tmpDataImportPath.getAbsolutePath() + "/" + DeliveryProperties.DEFAULT_FILENAME);
                    DeliveryProperties.write(deliveryFile, new DeliveryProperties.DeliveryMeta(deliveryKey, deliveryVersion, date, info));
                }

                @Override
                public boolean existsDeliveryVersion(String deliveryKey, String deliveryVersion) {
                    File deliveryVersionPath = getDataDeliveryVersionPath(deliveryKey, deliveryVersion);
                    return deliveryVersionPath.exists();
                }

                @Override
                public void onImport(ImportMetaFileInformation information, InputStream dataInputStream) throws FileChecksumException {
                    MessageDigest messageDigest = createMessageDigest(information);
                    InputStream dis = createDigestInputStream(dataInputStream, messageDigest);
                    FileOutputStream fos = null;
                    BufferedOutputStream bos = null;
                    try {
                        File tmpImportPathByType = getTmpImportPathByType(information);
                        mkdirs(tmpImportPathByType);
                        File file = new File(tmpImportPathByType.getAbsolutePath() + "/" + information.getFileName());
                        fos = new FileOutputStream(file);
                        bos = new BufferedOutputStream(fos);
                        IOUtils.copyLarge(dis, bos, 0l, information.getFileLength());
                    } catch (IOException e) {
                        throw new UnhandledException(e);
                    } finally {
                        IOUtils.closeQuietly(bos);
                        IOUtils.closeQuietly(fos);
                    }

                    if (messageDigest != null) {
                        String digest = Hex.encodeHexString(messageDigest.digest());
                        if (!digest.equals(information.getChecksum())) {
                            throw new FileChecksumException(
                              "Wrong checksum for + " + information.toString() + " Calculated checksum: " + digest);
                        }
                    }
                }

                private MessageDigest createMessageDigest(ImportMetaFileInformation information) {
                    ChecksumType checksumType = information.getChecksumType();
                    if (checksumType == ChecksumType.NONE) {
                        return null;
                    }

                    try {
                        return MessageDigest.getInstance(checksumType.getDigest());
                    } catch (NoSuchAlgorithmException e) {
                        throw new UnhandledException(e);
                    }
                }

                private InputStream createDigestInputStream(InputStream dataInputStream, MessageDigest digest) {
                    if (digest == null) {
                        return dataInputStream;
                    }
                    return new DigestInputStream(dataInputStream, digest);
                }

                @Override
                public void onCommit(String deliveryKey, String deliveryVersion, boolean activateChunk,
                  boolean activeVersion) {
                    log.info("Moving temporary data to final path");
                    moveDataFiles(deliveryKey, deliveryVersion);
                    log.info("Moving temporary index to final path");
                    moveIndexFiles(deliveryKey, deliveryVersion);
                    log.info("Write activation files");
                    activateDelivery(deliveryKey, deliveryVersion, activateChunk, activeVersion);
                    log.info("Cleaning up temporary stuff");
                    cleanupTemporaryFiles(deliveryKey, deliveryVersion);
                    log.info("Notifying JumboSearcher onDataChanged");
                    jumboSearcher.onDataChanged();
                    log.info("Import is finished");
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(databaseImportSession);
            IOUtils.closeQuietly(clientSocket);
        }
    }

    private void cleanupTemporaryFiles(String deliveryKey, String deliveryVersion) {
        File tmpDataDeliveryPath = getTmpDataDeliveryPath(deliveryKey, deliveryVersion);
        File dataChunkFolder = tmpDataDeliveryPath.getParentFile();
        if(dataChunkFolder.listFiles(StorageManagement.FOLDER_FILTER).length == 1) {
            deleteFolderIfExists(dataChunkFolder);
        } else {
            deleteFolderIfExists(tmpDataDeliveryPath);
        }

        File tmpIndexDeliveryPath = getTmpIndexDeliveryPath(deliveryKey, deliveryVersion);
        File indexChunkFolder = tmpIndexDeliveryPath.getParentFile();
        if(indexChunkFolder.listFiles(StorageManagement.FOLDER_FILTER).length == 1) {
            deleteFolderIfExists(indexChunkFolder);
        } else {
            deleteFolderIfExists(tmpIndexDeliveryPath);
        }
    }

    private void activateDelivery(String deliveryKey, String deliveryVersion, boolean activateChunk,
            boolean activateVersion) {
        File activeFile = new File(dataPath.getAbsolutePath() + "/" + deliveryKey + "/" + ActiveProperties.DEFAULT_FILENAME);
        if(activateVersion || !activeFile.exists()) {
            ActiveProperties.writeActiveFile(activeFile, deliveryVersion, activateChunk);
        }
        else {
            String currentDeliveryVersion = ActiveProperties.getActiveDeliveryVersion(activeFile);
            ActiveProperties.writeActiveFile(activeFile, currentDeliveryVersion, activateChunk);
        }
    }

    private void moveIndexFiles(String deliveryKey, String deliveryVersion) {
        File tmpIndexDeliveryPath = getTmpIndexDeliveryPath(deliveryKey, deliveryVersion);
        File finalIndexDeliveryPath = getIndexDeliveryVersionPath(deliveryKey, deliveryVersion);
        moveFolder(tmpIndexDeliveryPath, finalIndexDeliveryPath);
    }

    private void moveDataFiles(String deliveryKey, String deliveryVersion) {
        File tmpDataDeliveryPath = getTmpDataDeliveryPath(deliveryKey, deliveryVersion);
        File finalDataDeliveryPath = getDataDeliveryVersionPath(deliveryKey, deliveryVersion);
        moveFolder(tmpDataDeliveryPath, finalDataDeliveryPath);
    }

    private File getTmpDataDeliveryPath(String deliveryKey, String deliveryVersion) {
        return getTmpDeliveryPath(deliveryKey, deliveryVersion, dataPath);
    }

    private File getTmpIndexDeliveryPath(String deliveryKey, String deliveryVersion) {
        return getTmpDeliveryPath(deliveryKey, deliveryVersion, indexPath);
    }

    private File getTmpDeliveryPath(String deliveryKey, String deliveryVersion, File path) {
        String deliveryPath = getTemporaryPath(path)
          .getAbsolutePath() + "/" + deliveryKey + "/" + deliveryVersion + "/";
        return new File(deliveryPath);
    }

    private File getDataDeliveryVersionPath(String deliveryKey, String deliveryVersion) {
        return new File(dataPath.getAbsolutePath() + "/" + deliveryKey + "/" + deliveryVersion + "/");
    }


    private File getIndexDeliveryVersionPath(String deliveryKey, String deliveryVersion) {
        return new File(indexPath.getAbsolutePath() + "/" + deliveryKey + "/" + deliveryVersion + "/");
    }

    protected DatabaseImportSession createDatabaseImportSession() throws IOException {
        return new DatabaseImportSession(clientSocket, clientID);
    }

    private File getTemporaryPath(File path) {
        return new File(path.getAbsolutePath() + "/.tmp/");
    }

    protected File getTmpImportPathByType(ImportMetaFileInformation information) throws IOException {
        if (information.getFileType() == ImportMetaFileInformation.FileType.DATA) {
            File tmpDataDeliveryPath = getTmpDataDeliveryPath(information.getDeliveryKey(),
              information.getDeliveryVersion());
            return new File(tmpDataDeliveryPath.getAbsolutePath() + "/" + information.getCollection() + "/");
        } else if (information.getFileType() == ImportMetaFileInformation.FileType.INDEX) {
            File tmpIndexDeliveryPath = getTmpIndexDeliveryPath(information.getDeliveryKey(),
              information.getDeliveryVersion());
            return new File(
              tmpIndexDeliveryPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information
                .getIndexName() + "/");
        }
        throw new IllegalArgumentException(
          "Type " + information.getFileType() + " is not allowed, only data and index.");
    }


    private void deleteFolderIfExists(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            log.error("Error while deleting folder", e);
        }
    }

    private void mkdirs(File file) {
        if (file.exists()) {
            return;
        }
        if (!file.mkdirs()) {
            log.warn("Cannot create path: " + file.getAbsolutePath());
        }
    }

    private void moveFolder(File src, File dest) {
        if(!dest.getParentFile().exists()) {
            dest.getParentFile().mkdirs();
        }
        if (!src.renameTo(dest)) {
            log.warn("Cannot rename file: " + src.getAbsolutePath() + " to " + dest.getAbsolutePath());
        }
    }
}
