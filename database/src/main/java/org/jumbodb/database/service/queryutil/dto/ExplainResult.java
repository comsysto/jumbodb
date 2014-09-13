package org.jumbodb.database.service.queryutil.dto;

import org.jumbodb.common.query.JumboQuery;

import java.util.List;

/**
 * @author Carsten Hufe
 */
public class ExplainResult {
    private JumboQuery query;
    private String message;
    private List<String> executionPath;

    public ExplainResult(String message) {
        this.message = message;
    }

    public ExplainResult(JumboQuery query, List<String> executionPath) {
        this.query = query;
        this.executionPath = executionPath;
    }

    public String getMessage() {
        return message;
    }

    public JumboQuery getQuery() {
        return query;
    }

    public List<String> getExecutionPath() {
        return executionPath;
    }
}
