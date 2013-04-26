package org.jumbodb.connector.hadoop.configuration;

import java.util.List;

/**
 * User: carsten
 * Date: 4/17/13
 * Time: 3:49 PM
 */
public class IndexField {
    private String indexName;
    private List<String> fields;
    private String indexStrategy;
    private String datePattern;

    public IndexField() {
    }

    public IndexField(String indexName, List<String> fields, String indexStrategy) {
        this.indexName = indexName;
        this.fields = fields;
        this.indexStrategy = indexStrategy;
    }

    public IndexField(String indexName, List<String> fields, String indexStrategy, String datePattern) {
        this.indexName = indexName;
        this.fields = fields;
        this.indexStrategy = indexStrategy;
        this.datePattern = datePattern;
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

    public String getDatePattern() {
        return datePattern;
    }

    public void setDatePattern(String datePattern) {
        this.datePattern = datePattern;
    }


    @Override
    public String toString() {
        return "IndexField{" +
                "indexName='" + indexName + '\'' +
                ", fields=" + fields +
                ", indexStrategy='" + indexStrategy + '\'' +
                ", datePattern='" + datePattern + '\'' +
                '}';
    }
}
