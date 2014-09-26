package org.jumbodb.database.service.management.storage.dto.queryutil;

import org.apache.commons.lang.StringUtils;
import org.jumbodb.common.query.QueryOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class QueryUtilIndex implements Comparable<QueryUtilIndex> {
    private String name;
    private String strategy;
    private List<QueryOperation> supportedOperations;


    public QueryUtilIndex(String name, String strategy, List<QueryOperation> supportedOperations) {
        this.name = name;
        this.strategy = strategy;
        this.supportedOperations = supportedOperations;
    }

    public String getName() {
        return name;
    }

    public String getStrategy() {
        return strategy;
    }

    public List<QueryOperation> getSupportedOperations() {
        return supportedOperations;
    }

    public String getSupportedOperationsFormatted() {
        List<String> ops = new ArrayList<String>();
        for (QueryOperation supportedOperation : supportedOperations) {
            ops.add(supportedOperation.getOperation());
        }
        return StringUtils.join(ops, ", ");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryUtilIndex that = (QueryUtilIndex) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (strategy != null ? !strategy.equals(that.strategy) : that.strategy != null) return false;

        return true;
    }

    @Override
    public int compareTo(QueryUtilIndex o) {
        return name.compareTo(o.name);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "QueryUtilIndex{" +
                "name='" + name + '\'' +
                ", strategy='" + strategy + '\'' +
                ", supportedOperations=" + supportedOperations +
                '}';
    }
}
