package org.jumbodb.connector.query;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.connector.JumboConstants;
import org.jumbodb.connector.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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
    public static final String CMD_QUERY_JSON = ":cmd:query:json";
    public static final String CMD_QUERY_SQL = ":cmd:query:sql";
    private Logger log = LoggerFactory.getLogger(JumboQueryConnection.class);

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
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        return objectMapper;
    }

    public <T> Iterable<T> findWithStreamedResult(Class<T> jsonClazz, JumboQuery searchQuery) {
        final byte[] bytes = getBytesFromJumboQuery(searchQuery);
        return findWithStreamedResult(jsonClazz, bytes, CMD_QUERY_JSON);
    }

    public <T> Iterable<T> findWithStreamedResult(Class<T> jsonClazz, String searchQuery) {
        try {
            byte[] bytes = searchQuery.getBytes("UTF-8");
            return findWithStreamedResult(jsonClazz, bytes, CMD_QUERY_SQL);
        } catch (UnsupportedEncodingException e) {
            throw new UnhandledException(e);
        }
    }

    private <T> Iterable<T> findWithStreamedResult(final Class<T> jsonClazz, final byte[] bytes, final String type) {
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
                findWithCallback(jsonClazz, bytes, type, new ResultHandler<T>() {
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

    public <T> List<T> find(Class<T> jsonClazz, JumboQuery searchQuery) {
        final List<T> result = new LinkedList<T>();
        byte[] bytes = getBytesFromJumboQuery(searchQuery);
        findWithCallback(jsonClazz, bytes, CMD_QUERY_JSON, createResultHandler(result));
        return result;
    }

    // CARSTEN unit test
    public <T> List<T> find(Class<T> jsonClazz, String sql) {
        try {
            final List<T> result = new LinkedList<T>();
            findWithCallback(jsonClazz, sql.getBytes("UTF-8"), CMD_QUERY_SQL, createResultHandler(result));
            return result;
        } catch (UnsupportedEncodingException e) {
            throw new UnhandledException(e);
        }
    }

    private <T> ResultHandler<T> createResultHandler(final List<T> result) {
        return new ResultHandler<T>() {
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
        };
    }

    private byte[] getBytesFromJumboQuery(JumboQuery searchQuery) {
        try {
            return jsonMapper.writeValueAsBytes(searchQuery);
        } catch (JsonProcessingException e) {
            throw new UnhandledException(e);
        }
    }

    private <T> void findWithCallback(Class<T> jsonClazz, byte[] queryBytes, String queryType, ResultHandler<T> resultHandler) {
        long start = System.currentTimeMillis();
        Socket sock = null;
        OutputStream os = null;
        SnappyOutputStream sos = null;
        DataOutputStream dos = null;
        InputStream is = null;
        BufferedInputStream bis = null;
        SnappyInputStream sis = null;
        DataInputStream dis = null;
        int results = 0;
        try {
            sock = new Socket(host, port);

            os = sock.getOutputStream();
            sos = new SnappyOutputStream(os);
            dos = new DataOutputStream(sos);
            is = sock.getInputStream();
            bis = new BufferedInputStream(is);
            sis = new SnappyInputStream(bis);
            dis = new DataInputStream(sis);
            dos.writeInt(JumboConstants.QUERY_PROTOCOL_VERSION);
            dos.writeUTF(queryType);
            dos.writeInt(queryBytes.length);
            dos.write(queryBytes);
            dos.flush();
            // ende send query
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
                String message = dis.readUTF();
                log.error("Unknown error: " + message);
                throw new JumboUnknownException(message);
            }
            else if(cmd.equals(":error:wrongversion")) {
                String message = dis.readUTF();
                log.error("Wrong version: " + message);
                throw new JumboWrongVersionException(message);
            }
            else if(cmd.equals(":error:common")) {
                String message = dis.readUTF();
                log.error("Common error: " + message);
                throw new JumboCommonException(message);
            }
            else if(cmd.equals(":error:timeout")) {
                String message = dis.readUTF();
                log.error("Timeout: " + message);
                throw new JumboTimeoutException(message);
            }
            else if(cmd.equals(":error:collection:missing")) {
                log.warn("Collection is missing: " + dis.readUTF());
            }
            else if(cmd.equals(":error:collection:index:missing")) {
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
            IOUtils.closeQuietly(sos);
            IOUtils.closeQuietly(os);
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(sis);
            IOUtils.closeQuietly(bis);
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(sock);
        }
        log.info("Size " + results + " Time: " + (System.currentTimeMillis() - start));
//        return result;
    }

    public interface ResultHandler<T> {
        void onResult(T dataset);
        void onFinished();
        void onError();
    }
}
