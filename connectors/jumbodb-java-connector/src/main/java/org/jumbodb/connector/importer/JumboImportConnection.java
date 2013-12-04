package org.jumbodb.connector.importer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.connector.JumboConstants;
import org.jumbodb.connector.exception.JumboCommonException;
import org.jumbodb.connector.exception.JumboWrongVersionException;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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
    private SnappyOutputStream sos;
    private DataOutputStream dos;
    private InputStream is;
    private SnappyInputStream sis;
    private DataInputStream dis;

    public JumboImportConnection(String host, int port) {
        try {
            socket = new Socket(host, port);
            os = socket.getOutputStream();
            mcos = createMonitorCountingOutputStream(os);
            bos = new BufferedOutputStream(mcos);
            sos = new SnappyOutputStream(bos);
            dos = new DataOutputStream(sos);
            is = socket.getInputStream();
            sis = new SnappyInputStream(is);
            dis = new DataInputStream(sis);
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

    public void importIndex(IndexInfo indexInfo, OnCopyCallback callback) throws InvalidFileHashException {
        try {
            dos.writeUTF(":cmd:import:collection:index");
            dos.writeUTF(indexInfo.getCollection());
            dos.writeUTF(indexInfo.getIndexName());
            dos.writeUTF(indexInfo.getFilename());
            dos.writeLong(indexInfo.getFileLength());
            dos.writeUTF(indexInfo.getDeliveryKey());
            dos.writeUTF(indexInfo.getDeliveryVersion());
            dos.writeUTF(indexInfo.getIndexStrategy());
            dos.flush();
            String command = dis.readUTF();
            if(":copy".equals(command)) {
                String sha1Hash = callback.onCopy(dos);
                String afterCopyCommand = dis.readUTF();
                if(":verify:sha1".equals(afterCopyCommand)) {
                    String sha1HashRemote = dis.readUTF();
                    if(!sha1Hash.equals(sha1HashRemote)) {
                        throw new InvalidFileHashException("SHA-1 hash for index-file " + indexInfo.getCollection() + "/" + indexInfo.getIndexName() + "/" + indexInfo.getFilename() + " is invalid (local: " + sha1Hash + " / remote: " + sha1HashRemote + ")");
                    }
                }
                else {
                    handleErrors(afterCopyCommand);
                }
            } else {
                handleErrors(command);
            }
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private void handleErrors(String command) throws IOException {
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
        throw new JumboCommonException("Error on import [" + command + "]: " + message);
    }

    public void importData(DataInfo dataInfo, OnCopyCallback callback) throws InvalidFileHashException {
        try {
            dos.writeUTF(":cmd:import:collection:data");
            dos.writeUTF(dataInfo.getCollection());
            dos.writeUTF(dataInfo.getFilename());
            dos.writeLong(dataInfo.getFileLength());
            dos.writeUTF(dataInfo.getDeliveryKey());
            dos.writeUTF(dataInfo.getDeliveryVersion());
            dos.writeUTF(dataInfo.getDataStrategy());
            dos.flush();
            String command = dis.readUTF();
            if(":copy".equals(command)) {
                String sha1Hash = callback.onCopy(dos);
                String afterCopyCommand = dis.readUTF();
                if(":verify:sha1".equals(afterCopyCommand)) {
                    String sha1HashRemote = dis.readUTF();
                    if(!sha1Hash.equals(sha1HashRemote)) {
                        throw new InvalidFileHashException("SHA-1 hash for data-file " + dataInfo.getCollection() + "/" + dataInfo.getFilename() + " is invalid (local: " + sha1Hash + " / remote: " + sha1HashRemote + ")");
                    }
                }
                else {
                    handleErrors(afterCopyCommand);
                }
            }
            else {
                handleErrors(command);
            }
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public void sendMetaIndex(MetaIndex metaIndex) {
        try {
            dos.writeUTF(":cmd:import:collection:meta:index");
            dos.writeUTF(metaIndex.getCollection());
            dos.writeUTF(metaIndex.getDeliveryKey());
            dos.writeUTF(metaIndex.getDeliveryVersion());
            dos.writeUTF(metaIndex.getIndexName());
            dos.writeUTF(metaIndex.getIndexStrategy());
            dos.writeUTF(metaIndex.getIndexSourceFields());
            dos.flush();
            handleOkCommand();
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    private void handleOkCommand() throws IOException {
        String command = dis.readUTF();
        if(!":ok".equals(command)) {
            handleErrors(command);
        }
    }

    public void sendMetaData(MetaData metaData) {
        try {
            dos.writeUTF(":cmd:import:collection:meta:data");
            dos.writeUTF(metaData.getCollection());
            dos.writeUTF(metaData.getDeliveryKey());
            dos.writeUTF(metaData.getDeliveryVersion());
            dos.writeUTF(metaData.getDataStrategy());
            dos.writeUTF(metaData.getPath());
            dos.writeBoolean(metaData.isActivate());
            dos.writeUTF(metaData.getInfo());
            dos.flush();
            handleOkCommand();
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public long getByteCount() {
        return mcos.getByteCount();
    }

    public void sendFinishedNotification(String deliveryKey, String deliveryVersion) {
        try {
            dos.writeUTF(":cmd:import:finished");
            dos.writeUTF(deliveryKey);
            dos.writeUTF(deliveryVersion);
            dos.flush();
            handleOkCommand();
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(dos);
        IOUtils.closeQuietly(sos);
        IOUtils.closeQuietly(bos);
        IOUtils.closeQuietly(mcos);
        IOUtils.closeQuietly(os);
        IOUtils.closeQuietly(dis);
        IOUtils.closeQuietly(sis);
        IOUtils.closeQuietly(is);
        IOUtils.closeQuietly(socket);
    }
}
