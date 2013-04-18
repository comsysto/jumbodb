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
    private String strategy;

    public IndexJson() {
    }

    public IndexJson(String indexName, List<String> fields, String strategy) {
        this.indexName = indexName;
        this.fields = fields;
        this.strategy = strategy;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
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
                ", strategy='" + strategy + '\'' +
                '}';
    }
}
