package org.jumbodb.database.service.query;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.connector.exception.JumboUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(QueryTask.class);
    private Logger longRunningLog = LoggerFactory.getLogger("LONG_RUNNING_QUERY");

    private Socket clientSocket;
    private int clientID = -1;
    private JumboSearcher jumboSearcher;
    private final ObjectMapper jsonMapper;
    private ExecutorService queryTaskTimeoutExecutor;
    private long queryTimeoutInSeconds;
    private DatabaseQuerySession databaseQuerySession;
    private AtomicInteger numberOfResults = new AtomicInteger();
    private LinkedBlockingQueue<CancelableTask> cancelableTasks = new LinkedBlockingQueue<CancelableTask>();

    public QueryTask(Socket s, int clientID, JumboSearcher jumboSearcher, ObjectMapper jsonMapper, ExecutorService queryTaskTimeoutExecutor, long queryTimeoutInSeconds) {
        clientSocket = s;
        this.clientID = clientID;
        this.jumboSearcher = jumboSearcher;
        this.jsonMapper = jsonMapper;
        this.queryTaskTimeoutExecutor = queryTaskTimeoutExecutor;
        this.queryTimeoutInSeconds = queryTimeoutInSeconds;
    }

    @Override
    public void run() {
        log.debug("QueryServer - Accepted Client : ID - " + clientID + " : Address - " + clientSocket.getInetAddress().getHostName());
        try {
            databaseQuerySession = createDatabaseQuerySession();
            databaseQuerySession.query(new DatabaseQuerySession.QueryHandler() {
                @Override
                public int onQuery(String collection, byte[] query, final DatabaseQuerySession.ResultWriter resultWriter) {
                    Future<Integer> submit = queryTaskTimeoutExecutor.submit(new QueryTimeoutTask(collection, query, resultWriter));
                    try {
                        return submit.get(queryTimeoutInSeconds, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        cancelAllRunningTasks();
                        logQuery(e.getMessage(), collection, query);
                        throw new JumboUnknownException(e.getMessage());
                    } catch (ExecutionException e) {
                        cancelAllRunningTasks();
                        Throwable cause = e.getCause();
                        logQuery(cause.getMessage(), collection, query);
                        if(cause instanceof RuntimeException) {
                            throw (RuntimeException)cause;
                        }
                        throw new JumboUnknownException(cause.getMessage());
                    } catch (TimeoutException e) {
                        logQuery("Timed out after: " + queryTimeoutInSeconds + " seconds.", collection, query);
                        cancelAllRunningTasks();
                        throw new JumboTimeoutException("Timed out after: " + queryTimeoutInSeconds + " seconds.");
                    }
                }
            });
        } catch (IOException e) {
            log.error("Unhandled exception ", e);
        } finally {
            IOUtils.closeQuietly(databaseQuerySession);
            IOUtils.closeQuietly(clientSocket);
        }
    }

    private void logQuery(String errorMessage, String collection, byte[] query) {
        try {
            StringBuilder buf = new StringBuilder();
            buf.append("\n############################################################################\n");
            buf.append(errorMessage);
            buf.append("\n========================== Collection  ==============================\n");
            buf.append(collection);
            buf.append("\n========================== Jumbo DB Query ==================================\n");
            buf.append(new String(query, "UTF-8"));
            longRunningLog.warn(buf.toString());
        } catch(UnsupportedEncodingException e) {
            longRunningLog.warn("Could not log query: ", e);
            log.warn("Could not log query: ", e);
        }

    }

    private void cancelAllRunningTasks() {
        CancelableTask cancelableTask;
        try {
            while ((cancelableTask = cancelableTasks.poll(500, TimeUnit.MILLISECONDS)) != null) {
                cancelableTask.cancel();
            }
        } catch (InterruptedException e) {
            // nothing to do
        }
    }


    private class QueryTimeoutTask implements Callable<Integer> {
        private String collection;
        private byte[] query;
        private DatabaseQuerySession.ResultWriter resultWriter;

        private QueryTimeoutTask(String collection, byte[] query, DatabaseQuerySession.ResultWriter resultWriter) {
            this.collection = collection;
            this.query = query;
            this.resultWriter = resultWriter;
        }

        @Override
        public Integer call() {
            try {
                JumboQuery searchQuery = jsonMapper.readValue(query, JumboQuery.class);
                return jumboSearcher.findResultAndWriteIntoCallback(collection, searchQuery, new ResultCallback() {
                    @Override
                    public void writeResult(byte[] result) throws IOException {
                        resultWriter.writeResult(result);
                        numberOfResults.incrementAndGet();
                    }

                    @Override
                    public boolean needsMore(JumboQuery jumboQuery) throws IOException {
                        final int limit = jumboQuery.getLimit();
                        if (limit == -1) {
                            return true;
                        }
                        return numberOfResults.get() < limit;
                    }

                    @Override
                    public void collect(CancelableTask cancelableTask) {
                        cancelableTasks.add(cancelableTask);
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected DatabaseQuerySession createDatabaseQuerySession() throws IOException {
        return new DatabaseQuerySession(clientSocket, clientID);
    }
}