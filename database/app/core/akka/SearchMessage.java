package core.akka;

import core.query.OlchingQuery;

import java.io.DataOutputStream;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 3:24 PM
 */
public class SearchMessage {
    private String collectionName;
    private OlchingQuery searchQuery;

    public SearchMessage(String collectionName, OlchingQuery searchQuery) {
        this.collectionName = collectionName;
        this.searchQuery = searchQuery;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public OlchingQuery getSearchQuery() {
        return searchQuery;
    }
}
