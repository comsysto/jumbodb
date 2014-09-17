package org.jumbodb.database.service.query;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.connector.exception.JumboUnknownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTask implements Runnable {
    private Logger log = LoggerFactory.getLogger(QueryTask.class);
    private Logger longRunningLog = LoggerFactory.getLogger("LONG_RUNNING_QUERY");

    private Socket clientSocket;
    private int clientID = -1;
    private JumboSearcher jumboSearcher;
    private JumboQueryConverterService jumboQueryConverterService;
    private final ObjectMapper jsonMapper;
    private ExecutorService queryTaskTimeoutExecutor;
    private long queryTimeoutInSeconds;
    private DatabaseQuerySession databaseQuerySession;
    private AtomicInteger numberOfResults = new AtomicInteger();
    private LinkedBlockingQueue<CancelableTask> cancelableTasks = new LinkedBlockingQueue<CancelableTask>();

    public QueryTask(Socket s, int clientID, JumboSearcher jumboSearcher, JumboQueryConverterService jumboQueryConverterService,
                     ObjectMapper jsonMapper, ExecutorService queryTaskTimeoutExecutor, long queryTimeoutInSeconds) {
        clientSocket = s;
        this.clientID = clientID;
        this.jumboSearcher = jumboSearcher;
        this.jumboQueryConverterService = jumboQueryConverterService;
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
                public int onJsonQuery(byte[] query, final DatabaseQuerySession.ResultWriter resultWriter) {
                    try {
                        JumboQuery searchQuery = jsonMapper.readValue(query, JumboQuery.class);
                        Future<Integer> submit = queryTaskTimeoutExecutor.submit(new QueryTimeoutTask(searchQuery, resultWriter));
                        return submit.get(queryTimeoutInSeconds, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        cancelAllRunningTasks();
                        logQuery(e.getMessage(), query);
                        throw new JumboUnknownException(e.getMessage());
                    } catch (ExecutionException e) {
                        cancelAllRunningTasks();
                        Throwable cause = e.getCause();
                        logQuery(cause.getMessage(), query);
                        if(cause instanceof RuntimeException) {
                            throw (RuntimeException)cause;
                        }
                        throw new JumboUnknownException(cause.getMessage());
                    } catch (TimeoutException e) {
                        logQuery("Timed out after: " + queryTimeoutInSeconds + " seconds.", query);
                        cancelAllRunningTasks();
                        throw new JumboTimeoutException("Timed out after: " + queryTimeoutInSeconds + " seconds.");
                    } catch (JsonMappingException e) {
                        throw new JumboUnknownException(e.getMessage());
                    } catch (JsonParseException e) {
                        throw new JumboUnknownException(e.getMessage());
                    } catch (IOException e) {
                        throw new JumboUnknownException(e.getMessage());
                    }
                }

                // CARSTEN unit test
                @Override
                public int onSqlQuery(byte[] query, DatabaseQuerySession.ResultWriter resultWriter) {
                    try {
                        JumboQuery jumboQuery = jumboQueryConverterService.convertSqlToJumboQuery(new String(query, "UTF-8"));
                        Future<Integer> submit = queryTaskTimeoutExecutor.submit(new QueryTimeoutTask(jumboQuery, resultWriter));
                        return submit.get(queryTimeoutInSeconds, TimeUnit.SECONDS);
                    } catch (UnsupportedEncodingException e) {
                        throw new JumboUnknownException(e.getMessage());
                    } catch (InterruptedException e) {
                        cancelAllRunningTasks();
                        logQuery(e.getMessage(), query);
                        throw new JumboUnknownException(e.getMessage());
                    } catch (ExecutionException e) {
                        cancelAllRunningTasks();
                        Throwable cause = e.getCause();
                        logQuery(cause.getMessage(), query);
                        if(cause instanceof RuntimeException) {
                            throw (RuntimeException)cause;
                        }
                        throw new JumboUnknownException(cause.getMessage());
                    } catch (TimeoutException e) {
                        logQuery("Timed out after: " + queryTimeoutInSeconds + " seconds.", query);
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

    private void logQuery(String errorMessage, byte[] query) {
        try {
            StringBuilder buf = new StringBuilder();
            buf.append("\n############################################################################\n");
            buf.append(errorMessage);
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


    // CARSTEN static machen!
    private class QueryTimeoutTask implements Callable<Integer> {
        private ObjectMapper mapper = new ObjectMapper();
        private JumboQuery query;
        private DatabaseQuerySession.ResultWriter resultWriter;

        private QueryTimeoutTask(JumboQuery query, DatabaseQuerySession.ResultWriter resultWriter) {
            this.query = query;
            this.resultWriter = resultWriter;
        }

        @Override
        public Integer call() {
            // CARSTEN mehrere Implementierungen von ResultCallback machen und anhang von query die richtige auswaehlen
            // CARSTEN eine impl. für group by mit entsprechnder synchronisierung, möglicherweise Multimaps.synchronized nutzen
            // CARSTEN implementierung für groovy auswertung.
            return jumboSearcher.findResultAndWriteIntoCallback(query, new ResultCallback() {
                @Override
                public void writeResult(Map<String, Object> parsedJson) throws IOException {
                    // CARSTEN hier in seperater implementierung group auswertung.
                    // CARSTEN only for test implemented
                    // CARSTEN should be possible to use more and *
                    if(query.getSelectedFields().contains("*")) {
                        resultWriter.writeResult(mapper.writeValueAsBytes(parsedJson));
                    } else {
                        // CARSTEN when only one was selected don't write res0
                        // CARSTEN should be possible to use more
                        // CARSTEN find sub fields etc...
                        resultWriter.writeResult(mapper.writeValueAsBytes(parsedJson.get(query.getSelectedFields().get(0))));
                    }
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
        }
    }

    protected DatabaseQuerySession createDatabaseQuerySession() throws IOException {
        return new DatabaseQuerySession(clientSocket, clientID);
    }
}