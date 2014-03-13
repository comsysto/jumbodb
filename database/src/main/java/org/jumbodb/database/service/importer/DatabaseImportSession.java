package org.jumbodb.database.service.importer;

import org.apache.commons.io.IOUtils;
import org.jumbodb.connector.JumboConstants;
import org.jumbodb.common.query.ChecksumType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private DataInputStream dataInputStream;
    private OutputStream outputStream;
    private DataOutputStream dataOutputStream;


    public DatabaseImportSession(Socket clientSocket, int clientID) throws IOException {
        this.clientSocket = clientSocket;
        this.clientID = clientID;
    }

    public void runImport(ImportHandler importHandler) throws IOException {
        outputStream = clientSocket.getOutputStream();
        dataOutputStream = new DataOutputStream(outputStream);
        inputStream = clientSocket.getInputStream();
        bufferedInputStream = new BufferedInputStream(inputStream);
        dataInputStream = new DataInputStream(bufferedInputStream);

        int protocolVersion = dataInputStream.readInt();
        if(protocolVersion != JumboConstants.IMPORT_PROTOCOL_VERSION) {
            dataOutputStream.writeUTF(":error:wrongversion");
            dataOutputStream.writeUTF("Wrong protocol version - Got " + protocolVersion + ", but expected " + JumboConstants.IMPORT_PROTOCOL_VERSION);
            dataOutputStream.flush();
            return;
        }

        String cmd = dataInputStream.readUTF();
        log.info("ClientID " + clientID + " with command " + cmd);
        if(":cmd:import:init".equals(cmd)) {
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String date = dataInputStream.readUTF();
            String info = dataInputStream.readUTF();
            try {
                importHandler.onInit(deliveryKey, deliveryVersion, date, info);
                dataOutputStream.writeUTF(":success");
            } catch (DeliveryVersionExistsException e) {
                log.error("Existing delivery", e);
                dataOutputStream.writeUTF(":failed");
                dataOutputStream.writeUTF(e.getMessage());
            }
            dataOutputStream.flush();
        }
        else if(":cmd:import:collection:data".equals(cmd)) {
            ImportMetaFileInformation.FileType type = ImportMetaFileInformation.FileType.DATA;
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String collection = dataInputStream.readUTF();
            String filename = dataInputStream.readUTF();
            long fileLength = dataInputStream.readLong();
            ChecksumType checksumType = ChecksumType.valueOf(dataInputStream.readUTF());
            String checksum = checksumType != ChecksumType.NONE ? dataInputStream.readUTF() : null;
            ImportMetaFileInformation meta = new ImportMetaFileInformation(deliveryKey, deliveryVersion, collection,
                null, type, filename, fileLength, checksumType, checksum);
            try {
                importHandler.onImport(meta, dataInputStream);
                dataOutputStream.writeUTF(":success");
            } catch (FileChecksumException e) {
                log.error("Checksum exception", e);
                dataOutputStream.writeUTF(":error:checksum");
                dataOutputStream.writeUTF(e.getMessage());
            }
            dataOutputStream.flush();
        } else if(":cmd:import:collection:index".equals(cmd)) {
            ImportMetaFileInformation.FileType type = ImportMetaFileInformation.FileType.INDEX;
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            String collection = dataInputStream.readUTF();
            String indexName = dataInputStream.readUTF();
            String filename = dataInputStream.readUTF();
            long fileLength = dataInputStream.readLong();
            ChecksumType checksumType = ChecksumType.valueOf(dataInputStream.readUTF());
            String checksum = checksumType != ChecksumType.NONE ? dataInputStream.readUTF() : null;
            ImportMetaFileInformation meta = new ImportMetaFileInformation(deliveryKey, deliveryVersion, collection,
                    indexName, type, filename, fileLength, checksumType, checksum);
            try {
                importHandler.onImport(meta, dataInputStream);
                dataOutputStream.writeUTF(":success");
            } catch (FileChecksumException e) {
                log.error("Checksum exception", e);
                dataOutputStream.writeUTF(":error:checksum");
                dataOutputStream.writeUTF(e.getMessage());
            }
            dataOutputStream.flush();
        } else if(":cmd:import:delivery:version:exists".equals(cmd)) {
            String deliveryKey = dataInputStream.readUTF();
            String deliveryVersion = dataInputStream.readUTF();
            boolean b = importHandler.existsDeliveryVersion(deliveryKey, deliveryVersion);
            dataOutputStream.writeBoolean(b);
            dataOutputStream.flush();
        } else if(":cmd:import:commit".equals(cmd)) {
            log.info(cmd);
            boolean activateChunk = dataInputStream.readBoolean();
            boolean activateVersion = dataInputStream.readBoolean();
            importHandler.onCommit(dataInputStream.readUTF(), dataInputStream.readUTF(), activateChunk, activateVersion);
            dataOutputStream.writeUTF(":success");
            dataOutputStream.flush();
        } else {
            throw new UnsupportedOperationException("Unsupported command: " + cmd);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(dataInputStream);
        IOUtils.closeQuietly(bufferedInputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(dataOutputStream);
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(clientSocket);
    }
}
