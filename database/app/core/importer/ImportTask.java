package core.importer;

import core.query.QueryServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.xerial.snappy.SnappyOutputStream;
import play.Logger;

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ImportTask implements Runnable {
    private Socket clientSocket;
    private int clientID;
    private File dataPath;
    private File indexPath;
    private QueryServer queryServer;

    public ImportTask(Socket s, int i, File dataPath, File indexPath, QueryServer queryServer) {
        this.clientSocket = s;
        this.clientID = i;
        this.dataPath = dataPath;
        this.indexPath = indexPath;
        this.queryServer = queryServer;
    }

    public void run() {
        Logger.info("ImportServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress().getHostName());
        DatabaseImportSession databaseImportSession = null;
        try {
            databaseImportSession = new DatabaseImportSession(clientSocket, clientID);
            databaseImportSession.runImport(new ImportHandler() {
                @Override
                public void onImport(ImportMetaFileInformation information, InputStream dataInputStream) {
                    OutputStream fos = null;
                    DataOutputStream dos = null;
                    try {
                        String absoluteImportPath = getTemporaryImportAbsolutePathByType(information);
                        // CARSTEN abstract store format for index and data in own classes
                        // CARSTEN or single managing class

                        File storageFolderFile = new File(absoluteImportPath);
                        if (!storageFolderFile.exists()) {
                            storageFolderFile.mkdirs();
                        }
                        String filePlacePath = absoluteImportPath + information.getFileName();
                        File filePlacePathFile = new File(filePlacePath);
                        if (filePlacePathFile.exists()) {
                            filePlacePathFile.delete();
                        }
                        Logger.info("ImportServer - " + filePlacePath);


                        if (information.getFileType() == ImportMetaFileInformation.FileType.DATA) {
                            fos = new SnappyOutputStream(new BufferedOutputStream(new FileOutputStream(filePlacePathFile)));
                            dos = new DataOutputStream(fos);
                            dos.writeLong(information.getFileLength());
                            dos.flush();
                        } else {
                            fos = new FileOutputStream(filePlacePathFile);
                        }
                        IOUtils.copy(dataInputStream, fos);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        IOUtils.closeQuietly(dos);
                        IOUtils.closeQuietly(fos);
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
                    File activeDeliveryFile = new File(deliveryKeyPath + "/active.properties");
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
                    Properties active = new Properties();
                    active.setProperty("deliveryVersion", information.getDeliveryVersion());

                    File activeDeliveryFile = new File(activationPath.getAbsoluteFile() + "/" + information.getCollection());
                    FileOutputStream activeDeliveryFos = null;
                    try {
                        activeDeliveryFos = new FileOutputStream(activeDeliveryFile);
                        active.store(activeDeliveryFos, "Active Delivery");
                    } catch(IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        IOUtils.closeQuietly(activeDeliveryFos);
                    }
                }

                @Override
                public void onFinished(String deliveryKey, String deliveryVersion) {
                    Logger.info("Moving temporary data to final path");
                    moveDataFiles(deliveryKey, deliveryVersion);
                    Logger.info("Temporary data to final path moved");
                    Logger.info("Moving temporary index to final path");
                    moveIndexFiles(deliveryKey, deliveryVersion);
                    Logger.info("Temporary index to final path moved");
                    Logger.info("Moving activation files to final path");
                    moveActivationFiles(deliveryKey, deliveryVersion);
                    Logger.info("Activation files to final path moved");

                    Logger.info("Cleaning up temporary stuff");
                    try {
                        FileUtils.deleteDirectory(getTemporaryDeliveryPath(dataPath, deliveryKey, deliveryVersion));
                        FileUtils.deleteDirectory(getTemporaryDeliveryPath(indexPath, deliveryKey, deliveryVersion));
                    } catch (IOException e) {
                        Logger.error(e.toString());
                        throw new RuntimeException(e);
                    }
                    Logger.info("Temporary stuff cleaned up");
                    Logger.info("Restarting Query Server");
                    queryServer.restart();
                    Logger.info("Restarted Query Server");
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
