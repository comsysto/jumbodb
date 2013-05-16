package org.jumbodb.database.service.queryutil.dto;

import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class QueryResult {
    private String message = null;
    private List<Map<String, Object>> results;
    private long timeInMs;

    public QueryResult(String message) {
        this.message = message;
    }

    public QueryResult(List<Map<String, Object>> results, long timeInMs) {
        this.results = results;
        this.timeInMs = timeInMs;
    }

    public String getMessage() {
        return message;
    }

    public List<Map<String, Object>> getResults() {
        return results;
    }

    public long getTimeInMs() {
        return timeInMs;
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "results=" + results +
                ", timeInMs=" + timeInMs +
                '}';
    }
}
