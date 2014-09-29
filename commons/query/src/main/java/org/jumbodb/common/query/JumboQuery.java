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
    private List<IndexQuery> indexOrs = new LinkedList<IndexQuery>();
    private List<DataQuery> dataOrs = new LinkedList<DataQuery>();
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
        return this.indexOrs.add(indexComparision);
    }

    public boolean addDataQuery(DataQuery dataQuery) {
        return this.dataOrs.add(dataQuery);
    }

    public List<IndexQuery> getIndexOrs() {
        return indexOrs;
    }

    public void setIndexOrs(List<IndexQuery> indexOrs) {
        this.indexOrs = indexOrs;
    }

    public List<DataQuery> getDataOrs() {
        return dataOrs;
    }

    public void setDataOrs(List<DataQuery> dataOrs) {
        this.dataOrs = dataOrs;
    }

    @Override
    public String toString() {
        return "JumboQuery{" +
                "indexQuery=" + indexOrs +
                ", jsonQuery=" + dataOrs +
                ", limit=" + limit +
                ", resultCacheEnabled=" + resultCacheEnabled +
                '}';
    }
}

