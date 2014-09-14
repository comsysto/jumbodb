package org.jumbodb.database.service.queryutil;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.*;
import org.jumbodb.database.service.queryutil.dto.ExplainResult;
import org.jumbodb.database.service.queryutil.dto.QueryResult;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Carsten Hufe
 */
public class QueryUtilService {
    private JumboSearcher jumboSearcher;
    private JumboQueryConverterService jumboQueryConverterService;
    private ExecutorService executorService = Executors.newCachedThreadPool();

    @PreDestroy
    public void destroy() {
        executorService.shutdownNow();
    }

    public QueryResult findDocumentsByJsonQuery(String query, Integer defaultLimit) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JumboQuery jumboQuery = mapper.readValue(query, JumboQuery.class);
            if(jumboQuery.getLimit() == -1) {
                jumboQuery.setLimit(defaultLimit);
            }
            return findDocumentsByQuery(jumboQuery);
        } catch (Exception e) {
            return new QueryResult(e.getMessage());
        }
    }

    // CARSTEN unit test
    public QueryResult findDocumentsBySqlQuery(String sql, Integer defaultLimit) {
        try {
            JumboQuery jumboQuery = jumboQueryConverterService.convertSqlToJumboQuery(sql);
            if (jumboQuery.getLimit() == -1) {
                jumboQuery.setLimit(defaultLimit);
            }
            return findDocumentsByQuery(jumboQuery);
        } catch(Exception e) {
            return new QueryResult(e.getMessage());
        }

    }

    // CARSTEN unit test
    public ExplainResult explainSqlQuery(String sql) {
        try {
            JumboQuery jumboQuery = jumboQueryConverterService.convertSqlToJumboQuery(sql);
            return new ExplainResult(jumboQuery, Arrays.asList("add the executions")); // CARSTEN implement add the executions with chunk, version and size
        } catch(Exception e) {
            return new ExplainResult(e.getMessage());
        }

    }

    private QueryResult findDocumentsByQuery(final JumboQuery jumboQuery) {
        final ObjectMapper mapper = new ObjectMapper();
        long start = System.currentTimeMillis();
        final List<Map<String, Object>> result = new LinkedList<Map<String, Object>>();
        final List<CancelableTask> tasks = new LinkedList<CancelableTask>();

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                    jumboSearcher.findResultAndWriteIntoCallback(jumboQuery, new ResultCallback() {
                        @Override
                        public void writeResult(byte[] dataSet) throws IOException {
                            synchronized (result) {
                                result.add(mapper.readValue(dataSet, Map.class));
                            }
                        }

                        @Override
                        public void collect(CancelableTask cancelableTask) {
                            tasks.add(cancelableTask);
                        }

                        @Override
                        public boolean needsMore(JumboQuery jumboQuery) throws IOException {
                            return result.size() < jumboQuery.getLimit();
                        }
                    });

            }
        };
        Future<?> submit = executorService.submit(runnable);
        try {
            submit.get(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            cancelTasks(tasks);
            return new QueryResult(e.getMessage());
        } catch (ExecutionException e) {
            cancelTasks(tasks);
            return new QueryResult(e.getCause().getMessage());
        } catch (TimeoutException e) {
            cancelTasks(tasks);
            return new QueryResult(e.getMessage());
        }
        long queryTime = System.currentTimeMillis() - start;
        return new QueryResult(result, queryTime);
    }

    private void cancelTasks(List<CancelableTask> tasks) {
        for (CancelableTask task : tasks) {
            task.cancel();
        }
    }

    @Required
    public void setJumboSearcher(JumboSearcher jumboSearcher) {
        this.jumboSearcher = jumboSearcher;
    }

    @Required
    public void setJumboQueryConverterService(JumboQueryConverterService jumboQueryConverterService) {
        this.jumboQueryConverterService = jumboQueryConverterService;
    }
}
