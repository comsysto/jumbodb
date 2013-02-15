package core.importer;

import core.query.QueryServer;
import org.apache.commons.io.IOUtils;
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
                        String absoluteImportPath = getImportAbsolutePathByType(information);
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
                            // CARSTEN close cleanly
//                            fos = new SnappyOutputStream(new FileOutputStream(filePlacePathFile));
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
                    String deliveryKeyPath = dataPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information.getDeliveryKey();
                    Properties deliveryInfo = new Properties();
//                    deliveryInfo.setProperty("collection", information.getCollection());
//                    deliveryInfo.setProperty("deliveryKey", information.getDeliveryKey());
                    deliveryInfo.setProperty("deliveryVersion", information.getDeliveryVersion());
                    deliveryInfo.setProperty("sourcePath", information.getSourcePath());
                    deliveryInfo.setProperty("date", sdf.format(new Date()));
                    deliveryInfo.setProperty("info", information.getInfo());


                    String deliveryVersionPath = deliveryKeyPath + "/" + information.getDeliveryVersion();
                    File deliveryVersionFilePath = new File(deliveryVersionPath);
                    if(!deliveryVersionFilePath.exists()) {
                        deliveryVersionFilePath.mkdirs();
                    }
                    File deliveryInfoFile = new File(deliveryVersionPath + "/delivery.properties");
                    FileOutputStream deliveryInfoFos = null;
                    try {
                        deliveryInfoFos = new FileOutputStream(deliveryInfoFile);
                        deliveryInfo.store(deliveryInfoFos, "Delivery Information");
                    } catch(IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        IOUtils.closeQuietly(deliveryInfoFos);
                    }

                    File activeDeliveryFile = new File(deliveryKeyPath + "/active.properties");
                    if(!activeDeliveryFile.exists()) {
                        onActivateDelivery(information);
                    }
                }

                @Override
                public void onActivateDelivery(ImportMetaInformation information) {
                    String deliveryKeyPath = dataPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information.getDeliveryKey();
                    Properties active = new Properties();
//                    active.setProperty("collection", information.getCollection());
//                    active.setProperty("deliveryKey", information.getDeliveryKey());
                    active.setProperty("deliveryVersion", information.getDeliveryVersion());

                    File activeDeliveryFile = new File(deliveryKeyPath + "/active.properties");
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
                public void onFinished() {
                    Logger.info("should restart server ... onFinished");
                    queryServer.restart();
                }
            });
        } catch (IOException e) {
            System.err.println(e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(databaseImportSession);
        }
    }

    private String getImportAbsolutePathByType(ImportMetaFileInformation information) throws IOException {
        if (information.getFileType() == ImportMetaFileInformation.FileType.DATA) {
            return dataPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information.getDeliveryKey() + "/" + information.getDeliveryVersion() + "/";
        } else if (information.getFileType() == ImportMetaFileInformation.FileType.INDEX) {
            return indexPath.getAbsolutePath() + "/" + information.getCollection() + "/" + information.getDeliveryKey() + "/" + information.getDeliveryVersion() + "/" + information.getIndexName() + "/";
        }
        throw new IllegalArgumentException("Type " + information.getFileType() + " is not allowed, only data and index.");
    }
}
