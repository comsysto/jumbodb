package org.jumbodb.connector.query;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.xerial.snappy.SnappyInputStream;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 10:53 AM
 */
public class JumboQueryConnector {
    private static final Logger LOG = Logger.getLogger(JumboQueryConnector.class);
    private static final int PROTOCOL_VERSION = 2;
    private final String host;
    private final int port;
    private final ObjectMapper jsonMapper;
    private final long waitingTimeoutInMs = 300000;  // 5min kein ergebnis

    public JumboQueryConnector(String host, int port) {
        this.host = host;
        this.port = port;
        this.jsonMapper = new ObjectMapper();
        this.jsonMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public <T> Iterable<T> findWithStreamedResult(final String collection, final Class<T> jsonClazz, final JumboQuery searchQuery) {
        final AtomicBoolean finished = new AtomicBoolean(false);
        final LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<T>();
        JumboIterable<T> it = new JumboIterable<T>(new Iterator<T>() {

            @Override
            public boolean hasNext() {
                if(queue.size() > 0) {
                    return true;
                }
                long start = System.currentTimeMillis();
                while(queue.size() == 0 && !finished.get()) {
                    // wait nothing to do
                    long diffInTimeMillis = System.currentTimeMillis() - start;
                    if(diffInTimeMillis > waitingTimeoutInMs) {
                        throw new RuntimeException("Fetching results took longer than " + waitingTimeoutInMs + "ms");
                    }
                }
                return queue.size() > 0;
            }

            @Override
            public T next() {
                return queue.poll();
            }

            @Override
            public void remove() {
            }
        });
        new Thread() {
            @Override
            public void run() {
                findWithCallback(collection, jsonClazz, searchQuery, new ResultHandler<T>() {
                    @Override
                    public void onResult(T dataset) {
                        queue.add(dataset);
                    }

                    @Override
                    public void onFinished() {
                        finished.set(true);
                    }

                    @Override
                    public void onError() {
                        finished.set(true);
                    }
                });
            }
        }.start();

        return it;
    }

    public <T> List<T> find(String collection, Class<T> jsonClazz, JumboQuery searchQuery) {
        final List<T> result = new LinkedList<T>();
        findWithCallback(collection, jsonClazz, searchQuery, new ResultHandler<T>() {
            @Override
            public void onResult(T dataset) {
                result.add(dataset);
            }

            @Override
            public void onFinished() {
                // nothing to do
            }

            @Override
            public void onError() {
            }
        });
        return result;
    }

        // CARSTEN later support asyncronous calls
    private <T> void findWithCallback(String collection, Class<T> jsonClazz, JumboQuery searchQuery, ResultHandler<T> resultHandler) {
        long start = System.currentTimeMillis();
        Socket sock = null;
        OutputStream os = null;
        DataOutputStream dos = null;
        InputStream is = null;
        BufferedInputStream bufferedInputStream = null;
        SnappyInputStream snappyInputStream = null;
        DataInputStream dis = null;
//        List<T> result = new LinkedList<T>();
        int results = 0;
        try {
            sock = new Socket(host, port);

            os = sock.getOutputStream();
            dos = new DataOutputStream(os);
            is = sock.getInputStream();
            bufferedInputStream = new BufferedInputStream(is);
            snappyInputStream = new SnappyInputStream(bufferedInputStream);
            dis = new DataInputStream(snappyInputStream);
            int protocolVersion = dis.readInt();
            if(protocolVersion != PROTOCOL_VERSION) {
                throw new RuntimeException("Wrong protocol version. Got " + protocolVersion + ", but expected " + PROTOCOL_VERSION);
            }
            dos.writeUTF(":cmd:query");
            dos.writeUTF(collection);
            dos.writeUTF(jsonMapper.writeValueAsString(searchQuery));

            String s;
            while(!(s = dis.readUTF()).equals(":result:end")) {
//                result.add(jsonMapper.readValue(s, jsonClazz));
                resultHandler.onResult(jsonMapper.readValue(s, jsonClazz));
                results++;
            }
            resultHandler.onFinished();
        } catch (IOException e) {
            resultHandler.onError();
            throw new UnhandledException(e);
        } finally {
            IOUtils.closeQuietly(dos);
            IOUtils.closeQuietly(os);
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(bufferedInputStream);
            IOUtils.closeQuietly(snappyInputStream);
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(sock);
        }
        LOG.info("Size " + results + " Time: " + (System.currentTimeMillis() - start));
//        return result;
    }

    public interface ResultHandler<T> {
        void onResult(T dataset);
        void onFinished();
        void onError();
    }
}
