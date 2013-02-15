package core.akka;

import core.query.DumboQuery;

/**
 * User: carsten
 * Date: 2/5/13
 * Time: 3:24 PM
 */
public class SearchMessage {
    private String collectionName;
    private DumboQuery searchQuery;

    public SearchMessage(String collectionName, DumboQuery searchQuery) {
        this.collectionName = collectionName;
        this.searchQuery = searchQuery;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public DumboQuery getSearchQuery() {
        return searchQuery;
    }
}
