package org.jumbodb.database.service.importer;

import org.apache.commons.io.IOUtils;
import org.jumbodb.connector.JumboConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;

/**
 * User: carsten
 * Date: 2/7/13
 * Time: 11:06 AM
 */
public class DatabaseImportSession implements Closeable {
    private Logger log = LoggerFactory.getLogger(DatabaseImportSession.class);

    private Socket clientSocket;
    private int clientID;
    private InputStream inputStream;
    private BufferedInputStream bufferedInputStream;
    private SnappyInputStream snappyInputStream;
    private DataInputStream dataInputStream;
    private OutputStream outputStream;
    private SnappyOutputStream snappyOutputStream;
    private DataOutputStream dataOutputStream;


    public DatabaseImportSession(Socket clientSocket, int clientID) throws IOException {
        this.clientSocket = clientSocket;
        this.clientID = clientID;
    }

    public void runImport(ImportHandler importHandler) throws IOException {
        outputStream = clientSocket.getOutputStream();
        snappyOutputStream = new SnappyOutputStream(outputStream);
        dataOutputStream = new DataOutputStream(snappyOutputStream);
        inputStream = clientSocket.getInputStream();
        bufferedInputStream = new BufferedInputStream(inputStream);
        snappyInputStream = new SnappyInputStream(bufferedInputStream);
        dataInputStream = new DataInputStream(snappyInputStream);

        int protocolVersion = dataInputStream.readInt();
        if(protocolVersion != JumboConstants.IMPORT_PROTOCOL_VERSION) {
            dataOutputStream.writeUTF(":error:wrongversion");
            dataOutputStream.writeUTF("Wrong protocol version - Got " + protocolVersion + ", but expected " + JumboConstants.IMPORT_PROTOCOL_VERSION);
            dataOutputStream.flush();
            return;
        }

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
            dataOutputStream.writeUTF(":copy");
            dataOutputStream.flush();
            ImportMetaFileInformation meta = new ImportMetaFileInformation(type, filename, collection, null, fileLength, deliveryKey, deliveryVersion, dataStrategy);
            String sha1Hash = importHandler.onImport(meta, dataInputStream);
            dataOutputStream.writeUTF(":verify:sha1");
            dataOutputStream.writeUTF(sha1Hash);
            dataOutputStream.flush();
        } else if(":cmd:import:collection:index".equals(cmd)) {
            ImportMetaFileInformation.FileType type = ImportMetaFileInformation.FileType.INDEX;
            String collection = dataInputStream.readUTF();
            String indexName = dataInputStream.readUTF();
            String filename = dataInputStream.readUTF();
            long fileLength = dataInputStream.readLong();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String indexStrategy = dataInputStream.readUTF();
            dataOutputStream.writeUTF(":copy");
            dataOutputStream.flush();
            ImportMetaFileInformation meta = new ImportMetaFileInformation(type, filename, collection, indexName, fileLength, deliveryKey, deliveryVersion, indexStrategy);
            String sha1Hash = importHandler.onImport(meta, dataInputStream);
            dataOutputStream.writeUTF(":verify:sha1");
            dataOutputStream.writeUTF(sha1Hash);
            dataOutputStream.flush();
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
            dataOutputStream.writeUTF(":ok");
            dataOutputStream.flush();
        } else if(":cmd:import:collection:meta:index".equals(cmd)) {
            String collection = dataInputStream.readUTF();
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String indexName = dataInputStream.readUTF();
            String strategy = dataInputStream.readUTF();
            String indexSourceFields = dataInputStream.readUTF();
            ImportMetaIndex meta = new ImportMetaIndex(collection, deliveryKey, deliveryVersion, indexName, strategy, indexSourceFields);
            importHandler.onCollectionMetaIndex(meta);
            dataOutputStream.writeUTF(":ok");
            dataOutputStream.flush();
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
            dataOutputStream.writeUTF(":ok");
            dataOutputStream.flush();
        } else {
            throw new UnsupportedOperationException("Unsupported command: " + cmd);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(dataInputStream);
        IOUtils.closeQuietly(snappyInputStream);
        IOUtils.closeQuietly(bufferedInputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(dataOutputStream);
        IOUtils.closeQuietly(snappyOutputStream);
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(clientSocket);
    }
}
