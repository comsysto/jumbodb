package core.akka;

import core.query.JumboQuery;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 3:24 PM
 */
public class SearchMessage {
    private String collectionName;
    private JumboQuery searchQuery;

    public SearchMessage(String collectionName, JumboQuery searchQuery) {
        this.collectionName = collectionName;
        this.searchQuery = searchQuery;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public JumboQuery getSearchQuery() {
        return searchQuery;
    }
}
