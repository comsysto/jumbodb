package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.jumbodb.connector.JumboConstants;
import org.jumbodb.connector.exception.JumboUnknownException;
import org.jumbodb.database.service.statistics.GlobalStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: carsten
 * Date: 2/7/13
 * Time: 11:06 AM
 */
public class DatabaseQuerySession implements Closeable {
    private final Logger log = LoggerFactory.getLogger(DatabaseQuerySession.class);

    private Socket clientSocket;
    private InputStream inputStream;
    private SnappyInputStream snappyInputStream;
    private DataInputStream dataInputStream;
    private OutputStream outputStream;
    private BufferedOutputStream bufferedOutputStream;
    private SnappyOutputStream snappyOutputStream;
    private DataOutputStream dataOutputStream;

    public DatabaseQuerySession(Socket clientSocket, int clientID) throws IOException {
        this.clientSocket = clientSocket;
        inputStream = clientSocket.getInputStream();
        snappyInputStream = new SnappyInputStream(inputStream);
        dataInputStream = new DataInputStream(snappyInputStream);
        outputStream = clientSocket.getOutputStream();
        bufferedOutputStream = new BufferedOutputStream(outputStream);
        snappyOutputStream = new SnappyOutputStream(bufferedOutputStream);
        dataOutputStream = new DataOutputStream(snappyOutputStream);
        dataOutputStream.flush(); // cause snappy header
    }

    public void query(QueryHandler queryHandler) throws IOException {
        ResultWriter resultWriter = new ResultWriter();
        try {
            int protocolVersion = dataInputStream.readInt();
            if (protocolVersion != JumboConstants.QUERY_PROTOCOL_VERSION) {
                String error = "Wrong protocol version. Got " + protocolVersion + ", but expected " + JumboConstants.QUERY_PROTOCOL_VERSION;
                log.warn(error);
                dataOutputStream.writeInt(-1);
                dataOutputStream.writeUTF(":error:wrongversion");
                dataOutputStream.writeUTF(error);
                return;
            }
            String cmd = dataInputStream.readUTF();
            if (cmd.startsWith(":cmd:query")) {
                int size = dataInputStream.readInt();
                byte[] jsonQueryDocument = new byte[size];
                dataInputStream.readFully(jsonQueryDocument);
                long start = System.currentTimeMillis();
                resultWriter.start();
                int numberOfResults = 0;
                if (cmd.equals(":cmd:query:json")) {
                    numberOfResults = queryHandler.onJsonQuery(jsonQueryDocument, resultWriter);
                } else if (cmd.equals(":cmd:query:sql")) {
                    numberOfResults = queryHandler.onSqlQuery(jsonQueryDocument, resultWriter);
                } else {
                    throw new JumboCommonException("cmd " + cmd + " is not supported!");
                }
                GlobalStatistics.incNumberOfQueries(1l);
                GlobalStatistics.incNumberOfResults(numberOfResults);
                log.info("Full result in " + (System.currentTimeMillis() - start) + "ms with " + numberOfResults + " results");
                resultWriter.datasetsFinished();
                dataOutputStream.writeInt(-1); // After -1 command follows
                dataOutputStream.writeUTF(":result:end");
            }
        } catch (JumboTimeoutException e) {
            log.warn("Handled error through query", e);
            dataOutputStream.writeInt(-1);
            dataOutputStream.writeUTF(":error:timeout");
            dataOutputStream.writeUTF(e.getMessage());
        } catch (JumboCollectionMissingException e) {
            log.warn("Handled error through query", e);
            dataOutputStream.writeInt(-1);
            dataOutputStream.writeUTF(":error:collection:missing");
            dataOutputStream.writeUTF(e.getMessage());
        } catch (JumboIndexMissingException e) {
            log.warn("Handled error through query", e);
            dataOutputStream.writeInt(-1);
            dataOutputStream.writeUTF(":error:collection:index:missing");
            dataOutputStream.writeUTF(e.getMessage());
        } catch (JumboCommonException e) {
            log.warn("Handled error through query", e);
            dataOutputStream.writeInt(-1);
            dataOutputStream.writeUTF(":error:common");
            dataOutputStream.writeUTF(e.getMessage());
        } catch (EOFException e) {
            log.warn("Connection was unexpectly closed by the client.");
        } catch (JumboUnknownException e) {
            log.warn("Unhandled error", e);
            dataOutputStream.writeInt(-1);
            dataOutputStream.writeUTF(":error:unknown");
            dataOutputStream.writeUTF(e.getMessage());
        } catch (RuntimeException e) {
            log.warn("Unhandled error", e);
            dataOutputStream.writeInt(-1);
            dataOutputStream.writeUTF(":error:unknown");
            dataOutputStream.writeUTF("An unknown error occured on server side, check database log for further information: " + e.toString());
        } finally {
            dataOutputStream.flush();
            resultWriter.forceCleanup();
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(dataInputStream);
        IOUtils.closeQuietly(snappyInputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(dataOutputStream);
        IOUtils.closeQuietly(snappyOutputStream);
        IOUtils.closeQuietly(bufferedOutputStream);
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(clientSocket);
    }

    public class ResultWriter extends Thread {
        private LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<byte[]>();
        private boolean running = true;
        private AtomicInteger count = new AtomicInteger(0);

        public void writeResult(byte[] result) {
            try {
                int i = count.incrementAndGet();
                if (i % 50000 == 0) {
                    log.info("Results written to buffer: " + i + " Currently in buffer: " + queue.size());
                }
                queue.put(result);
            } catch (InterruptedException e) {
                log.info("Interrupted result writer");
            }
        }

        @Override
        public void run() {
            try {
                while (running || queue.size() > 0) {
                    byte dataset[] = queue.take();
                    if (dataset.length > 0) {
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
                while (queue.size() > 0) {
                    Thread.sleep(50);
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
        int onJsonQuery(byte[] query, DatabaseQuerySession.ResultWriter resultWriter);

        int onSqlQuery(byte[] query, DatabaseQuerySession.ResultWriter resultWriter);
    }
}
