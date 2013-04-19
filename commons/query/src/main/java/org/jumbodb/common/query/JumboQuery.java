package org.jumbodb.common.query;

import org.codehaus.jackson.annotate.JsonAutoDetect;

import java.util.*;

/**
 * User: carsten
 * Date: 11/23/12
 * Time: 1:05 PM
 */

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JumboQuery {
    private List<IndexQuery> indexQuery = new LinkedList<IndexQuery>();
    private List<JsonValueComparision> jsonComparision = new LinkedList<JsonValueComparision>();


    public boolean addIndexQuery(IndexQuery indexComparision) {
        return this.indexQuery.add(indexComparision);
    }

    public boolean addJsonComparision(JsonValueComparision indexComparision) {
        return this.jsonComparision.add(indexComparision);
    }

    public boolean addIndexQuery(String indexName, List<IndexClause> indexValues) {
        return addIndexQuery(new IndexQuery(indexName, indexValues));
    }

    public boolean addJsonComparision(JsonComparisionType comparisionType, String jsonPropertyName, List<Object> jsonValues) {
        return addJsonComparision(new JsonValueComparision(comparisionType, jsonPropertyName, jsonValues));
    }

    public List<IndexQuery> getIndexQuery() {
        return indexQuery;
    }

    public void setIndexQuery(List<IndexQuery> indexQuery) {
        this.indexQuery = indexQuery;
    }

    public List<JsonValueComparision> getJsonComparision() {
        return jsonComparision;
    }

    public void setJsonComparision(List<JsonValueComparision> jsonComparision) {
        this.jsonComparision = jsonComparision;
    }

    @Override
    public String toString() {
        return "JumboQuery{" +
                "indexQuery=" + indexQuery +
                ", jsonComparision=" + jsonComparision +
                '}';
    }
}

