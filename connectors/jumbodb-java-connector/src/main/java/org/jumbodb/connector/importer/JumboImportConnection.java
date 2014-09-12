package org.jumbodb.connector.importer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.ChecksumType;
import org.jumbodb.connector.JumboConstants;
import org.jumbodb.connector.exception.*;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;
import java.net.Socket;

/**
 * User: carsten
 * Date: 2/26/13
 * Time: 4:35 PM
 */
public class JumboImportConnection implements Closeable {
    private Socket socket;
    private OutputStream os;
    private MonitorCountingOutputStream mcos;
    private BufferedOutputStream bos;
    private DataOutputStream dos;
    private InputStream is;
    private DataInputStream dis;


    public JumboImportConnection(String host, int port) {
        try {
            socket = new Socket(host, port);
            os = socket.getOutputStream();
            mcos = createMonitorCountingOutputStream(os);
            bos = new BufferedOutputStream(mcos);
            dos = new DataOutputStream(bos);
            is = socket.getInputStream();
            dis = new DataInputStream(is);
            dos.writeInt(JumboConstants.IMPORT_PROTOCOL_VERSION);
            dos.flush();
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private MonitorCountingOutputStream createMonitorCountingOutputStream(final OutputStream outputStream) {
        return new MonitorCountingOutputStream(outputStream, 2000) {
            @Override
            protected void onInterval(long rateInBytesPerSecond, long copiedBytesSinceLastCall) {
                super.onInterval(rateInBytesPerSecond, copiedBytesSinceLastCall);
                onCopyRateUpdate(rateInBytesPerSecond, copiedBytesSinceLastCall);
            }
        };
    }

    protected void onCopyRateUpdate(long rateInBytesPerSecond, long copiedBytesSinceLastCall) {

    }

    public long getCopyRateInBytesPerSecond() {
        return mcos.getRateInBytesPerSecond();
    }

    public boolean existsDeliveryVersion(String deliveryChunkKey, String version) {
        try {
            dos.writeUTF(":cmd:import:delivery:version:exists");
            dos.writeUTF(deliveryChunkKey);
            dos.writeUTF(version);
            dos.flush();
            return dis.readBoolean();
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public void initImport(ImportInfo importInfo) {
        try {
            dos.writeUTF(":cmd:import:init");
            dos.writeUTF(importInfo.getDeliveryKey());
            dos.writeUTF(importInfo.getDeliveryVersion());
            dos.writeUTF(importInfo.getDate());
            dos.writeUTF(importInfo.getInfo());
            dos.flush();
            String command = dis.readUTF();
            handleErrors(command);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public void importIndexFile(IndexInfo indexInfo, OnCopyCallback callback) {
        try {
            dos.writeUTF(":cmd:import:collection:index");
            dos.writeUTF(indexInfo.getDeliveryKey());
            dos.writeUTF(indexInfo.getDeliveryVersion());
            dos.writeUTF(indexInfo.getCollection());
            dos.writeUTF(indexInfo.getIndexName());
            dos.writeUTF(indexInfo.getFileName());
            dos.writeLong(indexInfo.getFileLength());
            dos.writeUTF(indexInfo.getChecksumType().name());
            if(indexInfo.getChecksumType() != ChecksumType.NONE) {
                dos.writeUTF(indexInfo.getChecksum());
            }

            dos.flush();
            callback.onCopy(dos);

            String command = dis.readUTF();
            handleErrors(command);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private void handleErrors(String command) throws IOException {
        if(":success".equals(command)) {
            return;
        }
        if(command.startsWith(":error")) {
            handleError(command, dis.readUTF());
        }
        else {
            throw new IllegalStateException("Unknown command: " + command);
        }
    }

    private void handleError(String command, String message) {
        if(":error:wrongversion".equals(command)) {
            throw new JumboWrongVersionException(message);
        }
        else if(":error:checksum".equals(command)) {
            throw new JumboFileChecksumException(message);
        }
        else if(":error:deliveryversionexists".equals(command)) {
            throw new JumboDeliveryVersionExistsException(message);
        }
        else if(":error:unknown".equals(command)) {
            throw new JumboUnknownException(message);
        }
        throw new JumboCommonException("Error on import [" + command + "]: " + message);
    }

    public void importDataFile(DataInfo dataInfo, OnCopyCallback callback) {
        try {
            dos.writeUTF(":cmd:import:collection:data");
            dos.writeUTF(dataInfo.getDeliveryKey());
            dos.writeUTF(dataInfo.getDeliveryVersion());
            dos.writeUTF(dataInfo.getCollection());
            dos.writeUTF(dataInfo.getFileName());
            dos.writeLong(dataInfo.getFileLength());
            dos.writeUTF(dataInfo.getChecksumType().name());
            if(dataInfo.getChecksumType() != ChecksumType.NONE) {
                dos.writeUTF(dataInfo.getChecksum());
            }
            dos.flush();
            callback.onCopy(dos);
            String command = dis.readUTF();
            handleErrors(command);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public long getByteCount() {
        return mcos.getByteCount();
    }

    public void commitImport(String deliveryKey, String deliveryVersion, boolean activateChunk, boolean activateVersion) {
        try {
            dos.writeUTF(":cmd:import:commit");
            dos.writeUTF(deliveryKey);
            dos.writeUTF(deliveryVersion);
            dos.writeBoolean(activateChunk);
            dos.writeBoolean(activateVersion);
            dos.flush();
            String command = dis.readUTF();
            handleErrors(command);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public void close() throws IOException {
        dos.writeUTF(":cmd:close");
        dos.flush();
        IOUtils.closeQuietly(dos);
        IOUtils.closeQuietly(bos);
        IOUtils.closeQuietly(mcos);
        IOUtils.closeQuietly(os);
        IOUtils.closeQuietly(dis);
        IOUtils.closeQuietly(is);
        IOUtils.closeQuietly(socket);
    }
}
