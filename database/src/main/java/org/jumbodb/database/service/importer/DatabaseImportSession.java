package org.jumbodb.database.service.importer;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;
import java.net.Socket;

/**
 * User: carsten
 * Date: 2/7/13
 * Time: 11:06 AM
 */
public class DatabaseImportSession implements Closeable {
    private Logger log = LoggerFactory.getLogger(DatabaseImportSession.class);

    public static final int PROTOCOL_VERSION = 3;
    private Socket clientSocket;
    private int clientID;
    private InputStream inputStream;
    private SnappyInputStream snappyInputStream;
    private DataInputStream dataInputStream;
    private BufferedInputStream bufferedInputStream;
    private OutputStream outputStream;
    private DataOutputStream dataOutputStream;


    public DatabaseImportSession(Socket clientSocket, int clientID) throws IOException {
        this.clientSocket = clientSocket;
        this.clientID = clientID;
    }

    public void runImport(ImportHandler importHandler) throws IOException {
        outputStream = clientSocket.getOutputStream();
        dataOutputStream = new DataOutputStream(outputStream);
        dataOutputStream.writeInt(PROTOCOL_VERSION);
        dataOutputStream.flush();
        inputStream = clientSocket.getInputStream();
        dataInputStream = new DataInputStream(inputStream);
        String cmd = dataInputStream.readUTF();
        log.info("ClientID " + clientID + " with command " + cmd);
        if(":cmd:import:collection:data".equals(cmd)) {
            ImportMetaFileInformation.FileType type = ImportMetaFileInformation.FileType.DATA;
            String collection = dataInputStream.readUTF();
            String filename = dataInputStream.readUTF();
            long fileLength = dataInputStream.readLong();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String dataStrategy = dataInputStream.readUTF();
            ImportMetaFileInformation meta = new ImportMetaFileInformation(type, filename, collection, null, fileLength, deliveryKey, deliveryVersion, dataStrategy);
            bufferedInputStream = new BufferedInputStream(inputStream, 32768);
            snappyInputStream = new SnappyInputStream(bufferedInputStream);
            importHandler.onImport(meta, snappyInputStream);

        } else if(":cmd:import:collection:index".equals(cmd)) {
            ImportMetaFileInformation.FileType type = ImportMetaFileInformation.FileType.INDEX;
            String collection = dataInputStream.readUTF();
            String indexName = dataInputStream.readUTF();
            String filename = dataInputStream.readUTF();
            long fileLength = dataInputStream.readLong();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String indexStrategy = dataInputStream.readUTF();
            ImportMetaFileInformation meta = new ImportMetaFileInformation(type, filename, collection, indexName, fileLength, deliveryKey, deliveryVersion, indexStrategy);
            bufferedInputStream = new BufferedInputStream(inputStream, 32768);
            snappyInputStream = new SnappyInputStream(bufferedInputStream);
            importHandler.onImport(meta, snappyInputStream);

        } else if(":cmd:import:collection:meta:data".equals(cmd)) {
            String collection = dataInputStream.readUTF();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String dataStrategy = dataInputStream.readUTF();
            String sourcePath = dataInputStream.readUTF();
            boolean activate = dataInputStream.readBoolean();
            String info = dataInputStream.readUTF();
            ImportMetaData meta = new ImportMetaData(collection, deliveryKey, deliveryVersion, dataStrategy, sourcePath, info);
            importHandler.onCollectionMetaData(meta);
            if(activate) {
                importHandler.onActivateDelivery(meta);
            }
        } else if(":cmd:import:collection:meta:index".equals(cmd)) {
            String collection = dataInputStream.readUTF();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String indexName = dataInputStream.readUTF();
            String strategy = dataInputStream.readUTF();
            String indexSourceFields = dataInputStream.readUTF();
            ImportMetaIndex meta = new ImportMetaIndex(collection, deliveryKey, deliveryVersion, indexName, strategy, indexSourceFields);
            importHandler.onCollectionMetaIndex(meta);
        } else if(":cmd:import:delivery:version:exists".equals(cmd)) {
//            String collection = dataInputStream.readUTF();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            boolean b = importHandler.existsDeliveryVersion(deliveryKey, deliveryVersion);
            dataOutputStream.writeBoolean(b);
            dataOutputStream.flush();
        } else if(":cmd:import:finished".equals(cmd)) {
            log.info(":cmd:import:finished");
            importHandler.onFinished(dataInputStream.readUTF(), dataInputStream.readUTF());
        } else {
            throw new UnsupportedOperationException("Unsupported command: " + cmd);
        }

    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(snappyInputStream);
        IOUtils.closeQuietly(dataInputStream);
        IOUtils.closeQuietly(bufferedInputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(dataOutputStream);
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(clientSocket);
    }
}
