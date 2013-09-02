package org.jumbodb.database.service.queryutil;

import org.apache.commons.lang.UnhandledException;
import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.database.service.query.CancelableTask;
import org.jumbodb.database.service.query.JumboSearcher;
import org.jumbodb.database.service.query.ResultCallback;
import org.jumbodb.database.service.queryutil.dto.QueryResult;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class QueryUtilService {
    private JumboSearcher jumboSearcher;

    public QueryResult findDocumentsByQuery(String collection, String query) {
        final ObjectMapper mapper = new ObjectMapper();
        long start = System.currentTimeMillis();
        final List<Map<String, Object>> result = new LinkedList<Map<String, Object>>();
        try {
            final JumboQuery jumboQuery = mapper.readValue(query, JumboQuery.class);
            if(jumboQuery.getLimit() < 0) {
                jumboQuery.setLimit(20);
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
                    // TODO implement timeout handling in this service
                }

                @Override
                public boolean needsMore(JumboQuery jumboQuery) throws IOException {
                    return result.size() < jumboQuery.getLimit();
                }
            });


        } catch (IOException e) {
            return new QueryResult(e.getMessage());
        }
        long queryTime = System.currentTimeMillis() - start;
        return new QueryResult(result, queryTime);
    }

    public void findDocumentsByQuery(String collection, String query, ResultCallback callback) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final JumboQuery jumboQuery = mapper.readValue(query, JumboQuery.class);
            jumboSearcher.findResultAndWriteIntoCallback(collection, jumboQuery, callback);

        } catch (IOException e) {
            throw new UnhandledException(e);
        }
    }

    @Required
    public void setJumboSearcher(JumboSearcher jumboSearcher) {
        this.jumboSearcher = jumboSearcher;
    }
}
