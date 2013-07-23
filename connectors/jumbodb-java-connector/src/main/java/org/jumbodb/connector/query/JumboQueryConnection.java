package org.jumbodb.connector.query;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.connector.JumboConstants;
import org.jumbodb.connector.exception.JumboCommonException;
import org.jumbodb.connector.exception.JumboIndexMissingException;
import org.jumbodb.connector.exception.JumboUnknownException;
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
public class JumboQueryConnection {
    private static final Logger LOG = Logger.getLogger(JumboQueryConnection.class);
    private final String host;
    private final int port;
    private final ObjectMapper jsonMapper;

    public JumboQueryConnection(String host, int port) {
        this.host = host;
        this.port = port;
        this.jsonMapper = createJacksonObjectMapper();
    }

    protected ObjectMapper createJacksonObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        return objectMapper;
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
                    if(diffInTimeMillis > getTimeoutInMs()) {
                        throw new RuntimeException("Fetching results took longer than " + getTimeoutInMs() + "ms");
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
                        try {
                            queue.put(dataset);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
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

    public long getTimeoutInMs() {
        return JumboConstants.QUERY_WAITING_TIMEOUT_IN_MS;
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
            if(protocolVersion != JumboConstants.QUERY_PROTOCOL_VERSION) {
                throw new RuntimeException("Wrong protocol version. Got " + protocolVersion + ", but expected " + JumboConstants.QUERY_PROTOCOL_VERSION);
            }
            dos.writeUTF(":cmd:query");
            dos.writeUTF(collection);
            byte[] queryBytes = jsonMapper.writeValueAsBytes(searchQuery);
            dos.writeInt(queryBytes.length);
            dos.write(queryBytes);

            int byteArrayLength;
            byte[] jsonByteArray = new byte[1024];
            while((byteArrayLength = dis.readInt()) > -1) {
                // reassign and make bigger
                if(byteArrayLength > jsonByteArray.length) {
                    jsonByteArray = new byte[byteArrayLength];
                }
                dis.readFully(jsonByteArray, 0, byteArrayLength);
                T result = jsonMapper.readValue(jsonByteArray, 0, byteArrayLength, jsonClazz);
                resultHandler.onResult(result);
                results++;
            }
            String cmd = dis.readUTF();
            if(cmd.equals(":error:unknown")) {
                throw new JumboUnknownException(dis.readUTF());
            }
            else if(cmd.equals(":error:common")) {
                throw new JumboCommonException(dis.readUTF());
            } else if(cmd.equals(":error:collection:missing")) {
                LOG.warn("Collection is missing: " + dis.readUTF());
//                throw new JumboCollectionMissingException(dis.readUTF());
            } else if(cmd.equals(":error:collection:index:missing")) {
                throw new JumboIndexMissingException(dis.readUTF());
            }

            else if(!cmd.equals(":result:end")) {
                throw new IllegalStateException("After length -1 should :result:end must follow!");
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
