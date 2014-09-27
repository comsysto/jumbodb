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

    private List<SelectField> selectedFields = new LinkedList<SelectField>();
    private List<IndexQuery> indexQuery = new LinkedList<IndexQuery>();
    private List<DataQuery> dataQuery = new LinkedList<DataQuery>();
    private List<String> groupByFields = new LinkedList<String>();
    private List<OrderField> orderBy = new LinkedList<OrderField>();
    private int limit = -1;
    private boolean resultCacheEnabled = true;

    public List<SelectField> getSelectedFields() {
        return selectedFields;
    }

    public void setSelectedFields(List<SelectField> selectedFields) {
        this.selectedFields = selectedFields;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public List<OrderField> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<OrderField> orderBy) {
        this.orderBy = orderBy;
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

    public boolean addDataQuery(DataQuery dataQuery) {
        return this.dataQuery.add(dataQuery);
    }

    public List<IndexQuery> getIndexQuery() {
        return indexQuery;
    }

    public void setIndexQuery(List<IndexQuery> indexQuery) {
        this.indexQuery = indexQuery;
    }

    public List<DataQuery> getDataQuery() {
        return dataQuery;
    }

    public void setDataQuery(List<DataQuery> dataQuery) {
        this.dataQuery = dataQuery;
    }

    @Override
    public String toString() {
        return "JumboQuery{" +
                "indexQuery=" + indexQuery +
                ", jsonQuery=" + dataQuery +
                ", limit=" + limit +
                ", resultCacheEnabled=" + resultCacheEnabled +
                '}';
    }
}

