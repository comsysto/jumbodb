package org.jumbodb.connector.hadoop.index.json;

import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 3:49 PM
 */
public class IndexJson {
    private String indexName;
    private List<String> fields;
    private String indexStrategy;

    public IndexJson() {
    }

    public IndexJson(String indexName, List<String> fields, String indexStrategy) {
        this.indexName = indexName;
        this.fields = fields;
        this.indexStrategy = indexStrategy;
    }

    public String getIndexStrategy() {
        return indexStrategy;
    }

    public void setIndexStrategy(String indexStrategy) {
        this.indexStrategy = indexStrategy;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "IndexJson{" +
                "indexName='" + indexName + '\'' +
                ", fields=" + fields +
                ", indexStrategy='" + indexStrategy + '\'' +
                '}';
    }
}
