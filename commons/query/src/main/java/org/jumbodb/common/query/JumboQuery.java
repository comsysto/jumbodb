package org.jumbodb.common.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedList;
import java.util.List;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 1:05 PM
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JumboQuery {
    private String collection;
    private List<String> selectedFields = new LinkedList<String>();
    private List<IndexQuery> indexQuery = new LinkedList<IndexQuery>();
    private List<DataQuery> jsonQuery = new LinkedList<DataQuery>();
    private int limit = -1;
    private boolean resultCacheEnabled = true;

    public List<String> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(List<String> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public boolean isResultCacheEnabled() {
        return resultCacheEnabled;
    }

    public void setResultCacheEnabled(boolean resultCacheEnabled) {
        this.resultCacheEnabled = resultCacheEnabled;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean addIndexQuery(IndexQuery indexComparision) {
        return this.indexQuery.add(indexComparision);
    }

//    public boolean addIndexQuery(String indexName, List<QueryClause> indexValues) {
//        return addIndexQuery(new IndexQuery(indexName, indexValues));
//    }

    public boolean addJsonQuery(DataQuery jsonQuery) {
        return this.jsonQuery.add(jsonQuery);
    }

//    public boolean addJsonQuery(String fieldName, List<QueryClause> indexValues) {
//        return addJsonQuery(new JsonQuery(fieldName, indexValues));
//    }

    public List<IndexQuery> getIndexQuery() {
        return indexQuery;
    }

    public void setIndexQuery(List<IndexQuery> indexQuery) {
        this.indexQuery = indexQuery;
    }

    public List<DataQuery> getJsonQuery() {
        return jsonQuery;
    }

    public void setJsonQuery(List<DataQuery> jsonQuery) {
        this.jsonQuery = jsonQuery;
    }

    @Override
    public String toString() {
        return "JumboQuery{" +
                "indexQuery=" + indexQuery +
                ", jsonQuery=" + jsonQuery +
                ", limit=" + limit +
                ", resultCacheEnabled=" + resultCacheEnabled +
                '}';
    }
}

