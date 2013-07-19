package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.jumbodb.database.service.statistics.GlobalStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: carsten
 * Date: 2/7/13
 * Time: 11:06 AM
 */
public class DatabaseQuerySession implements Closeable {
    private final Logger log = LoggerFactory.getLogger(DatabaseQuerySession.class);

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
            long start = System.currentTimeMillis();
            ResultWriter resultWriter = new ResultWriter();
            resultWriter.start();
            try {
                int numberOfResults = queryHandler.onQuery(collection, jsonQueryDocument, resultWriter);
                GlobalStatistics.incNumberOfQueries(1l);
                GlobalStatistics.incNumberOfResults(numberOfResults);
                log.info("Full result in " + (System.currentTimeMillis() - start) + "ms with " + numberOfResults + " results");
                resultWriter.datasetsFinished();
                dataOutputStream.writeInt(-1); // After -1 command follows
                dataOutputStream.writeUTF(":result:end");
            } catch(JumboCollectionMissingException e) {
                log.warn("Handled error through query", e);
                dataOutputStream.writeInt(-1);
                dataOutputStream.writeUTF(":error:collection:missing");
                dataOutputStream.writeUTF(e.getMessage());
            } catch(JumboIndexMissingException e) {
                log.warn("Handled error through query", e);
                dataOutputStream.writeInt(-1);
                dataOutputStream.writeUTF(":error:collection:index:missing");
                dataOutputStream.writeUTF(e.getMessage());
            } catch(JumboCommonException e) {
                log.warn("Handled error through query", e);
                dataOutputStream.writeInt(-1);
                dataOutputStream.writeUTF(":error:common");
                dataOutputStream.writeUTF(e.getMessage());
            } catch(RuntimeException e) {
                log.warn("Unhandled error", e);
                dataOutputStream.writeInt(-1);
                dataOutputStream.writeUTF(":error:unknown");
                dataOutputStream.writeUTF("An unknown error occured on server side, check database log for further information: " + e.toString());
            } finally {
                resultWriter.forceCleanup();
            }

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

    public class ResultWriter extends Thread {
        private LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
        private boolean running = true;
        private AtomicInteger count = new AtomicInteger(0);
        public void writeResult(byte[] result) {
            try {
                int i = count.incrementAndGet();
                if(i % 50000 == 0) {
                    log.info("Results written to buffer: " + i + " Currently in buffer: " + queue.size());
                }
                queue.put(result);
            } catch (InterruptedException e) {
                log.error("Unhandled error", e);
            }
        }

        @Override
        public void run() {
            try {
                while(running || queue.size() > 0) {
                    byte dataset[] = queue.take();
                    if(dataset.length > 0) {
                        dataOutputStream.writeInt(dataset.length);
                        dataOutputStream.write(dataset);
                    }
                }
            } catch (InterruptedException e) {
                log.error("Unhandled error", e);
                forceCleanup();
            } catch (IOException e) {
                log.error("Unhandled error", e);
                forceCleanup();
            }
        }

        public void datasetsFinished() {
            try {
                running = false;
                queue.put(new byte[0]);
                while(queue.size() > 0) {
                        Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                log.error("Unhandled", e);
            }
        }

        public void forceCleanup() {
            running = false;
            queue.clear();
        }
    }

    public interface QueryHandler {
        int onQuery(String collection, byte[] query, DatabaseQuerySession.ResultWriter resultWriter);
    }
}
