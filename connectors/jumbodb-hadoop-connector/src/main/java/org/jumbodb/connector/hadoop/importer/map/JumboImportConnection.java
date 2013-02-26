package org.jumbodb.connector.hadoop.importer.map;

import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.io.IOUtils;
import org.jumbodb.connector.hadoop.JumboConstants;
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
    private OutputStream outputStream;
    private BufferedOutputStream bufferedOutputStream;
    private SnappyOutputStream snappyOutputStream;
    private DataOutputStream dos;
    private DataInputStream dis;

    public JumboImportConnection(String host, int port) {
        try {
            socket = new Socket(host, port);
            outputStream = socket.getOutputStream();
            dos = new DataOutputStream(outputStream);
            bufferedOutputStream = new BufferedOutputStream(outputStream, JumboConstants.BUFFER_SIZE);   // make configurable
            snappyOutputStream = new SnappyOutputStream(bufferedOutputStream);
            dis = new DataInputStream(socket.getInputStream());
            int protocolVersion = dis.readInt();
            if(protocolVersion != JumboConstants.PROTOCOL_VERSION) {
                throw new RuntimeException("Wrong protocol version - Got " + protocolVersion + ", but expected " + JumboConstants.PROTOCOL_VERSION);
            }
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public void importIndex(IndexInfo indexInfo, OnCopyCallback callback) {
        try {
            dos.writeUTF(":cmd:import:collection:index");
            dos.writeUTF(indexInfo.getCollection());
            dos.writeUTF(indexInfo.getIndexName());
            dos.writeUTF(indexInfo.getFilename());
            dos.writeLong(indexInfo.getFileLength());
            dos.writeUTF(indexInfo.getDeliveryKey());
            dos.writeUTF(indexInfo.getDeliveryVersion());
            dos.flush();
            callback.onCopy(snappyOutputStream);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    public void importData(DataInfo dataInfo, OnCopyCallback callback) {
        try {
            dos.writeUTF(":cmd:import:collection:data");
            dos.writeUTF(dataInfo.getCollection());
            dos.writeUTF(dataInfo.getFilename());
            dos.writeLong(dataInfo.getFileLength());
            dos.writeUTF(dataInfo.getDeliveryKey());
            dos.writeUTF(dataInfo.getDeliveryVersion());
            dos.flush();
            callback.onCopy(snappyOutputStream);
        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(bufferedOutputStream);
        IOUtils.closeStream(snappyOutputStream);
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(dos);
        IOUtils.closeStream(dis);
        IOUtils.closeSocket(socket);
    }
}
