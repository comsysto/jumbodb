package org.jumbodb.database.service.queryutil;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.CancelableTask;
import org.jumbodb.database.service.query.JumboSearcher;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.queryutil.dto.QueryResult;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Carsten Hufe
 */
public class QueryUtilService {
    private JumboSearcher jumboSearcher;
    private ExecutorService executorService = Executors.newCachedThreadPool();

    @PreDestroy
    public void destroy() {
        executorService.shutdownNow();
    }

    public QueryResult findDocumentsByQuery(final String collection, final String query, final Integer defaultLimit) {
        final ObjectMapper mapper = new ObjectMapper();
        long start = System.currentTimeMillis();
        final List<Map<String, Object>> result = new LinkedList<Map<String, Object>>();
        final List<CancelableTask> tasks = new LinkedList<CancelableTask>();

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    final JumboQuery jumboQuery = mapper.readValue(query, JumboQuery.class);
                    if(jumboQuery.getLimit() == -1) {
                        jumboQuery.setLimit(defaultLimit);
                    }
                    jumboSearcher.findResultAndWriteIntoCallback(collection, jumboQuery, new ResultCallback() {
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


                } catch (IOException e) {
                    throw new UnhandledException(e);
                }
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

//    public void findDocumentsByQuery(String collection, String query, ResultCallback callback) {
//        final ObjectMapper mapper = new ObjectMapper();
//        try {
//            final JumboQuery jumboQuery = mapper.readValue(query, JumboQuery.class);
//            jumboSearcher.findResultAndWriteIntoCallback(collection, jumboQuery, callback);
//
//        } catch (IOException e) {
//            throw new UnhandledException(e);
//        }
//    }

    @Required
    public void setJumboSearcher(JumboSearcher jumboSearcher) {
        this.jumboSearcher = jumboSearcher;
    }
}
