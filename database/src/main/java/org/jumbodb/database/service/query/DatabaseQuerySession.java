package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.jumbodb.database.service.statistics.GlobalStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;

/**
 * User: carsten
 * Date: 2/7/13
 * Time: 11:06 AM
 */
public class DatabaseQuerySession implements Closeable {
    private Logger log = LoggerFactory.getLogger(DatabaseQuerySession.class);

    public static final int PROTOCOL_VERSION = 3;
    private Socket clientSocket;
    private InputStream inputStream;
    private DataInputStream dataInputStream;
    private OutputStream outputStream;
    private DataOutputStream dataOutputStream;
    private BufferedOutputStream bufferedOutputStream;
    private SnappyOutputStream snappyOutputStream;

    public DatabaseQuerySession(Socket clientSocket, int clientID) throws IOException {
        this.clientSocket = clientSocket;
        inputStream = clientSocket.getInputStream();
        dataInputStream = new DataInputStream(inputStream);
        outputStream = clientSocket.getOutputStream();
        bufferedOutputStream = new BufferedOutputStream(outputStream);
        snappyOutputStream = new SnappyOutputStream(bufferedOutputStream);
        dataOutputStream = new DataOutputStream(snappyOutputStream);
    }

    public void query(QueryHandler queryHandler) throws IOException {
        dataOutputStream.writeInt(PROTOCOL_VERSION);
        dataOutputStream.flush();
        snappyOutputStream.flush();
        String cmd = dataInputStream.readUTF();
        if (cmd.equals(":cmd:query")) {
            String collection = dataInputStream.readUTF();
            log.info("Collection: " + collection);
            int size = dataInputStream.readInt();
            byte[] jsonQueryDocument = new byte[size];
            dataInputStream.readFully(jsonQueryDocument);
//            String jsonQueryString = dataInputStream.readUTF();
//            Logger.info("Query: " + jsonQueryString);
            long start = System.currentTimeMillis();
            int numberOfResults = queryHandler.onQuery(collection, jsonQueryDocument, new ResultWriter());
            GlobalStatistics.incNumberOfQueries(1l);
            GlobalStatistics.incNumberOfResults(numberOfResults);
            log.info("Full result in " + (System.currentTimeMillis() - start) + "ms with " + numberOfResults + " results");
            dataOutputStream.writeInt(-1); // After -1 command follows
            dataOutputStream.writeUTF(":result:end");
            dataOutputStream.flush();
            snappyOutputStream.flush();
        }

    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(dataInputStream);
        IOUtils.closeQuietly(bufferedOutputStream);
        IOUtils.closeQuietly(snappyOutputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(dataOutputStream);
        IOUtils.closeQuietly(clientSocket);
    }

    public class ResultWriter {
        public synchronized void writeResult(byte[] result) throws IOException {
            dataOutputStream.writeInt(result.length);
            dataOutputStream.write(result);
        }
    }

    public interface QueryHandler {
        int onQuery(String collection, byte[] query, ResultWriter resultWriter);
    }
}
