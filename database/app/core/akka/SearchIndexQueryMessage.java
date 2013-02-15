package core.akka;

import core.query.DumboQuery;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 2/5/13
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class SearchIndexQueryMessage {
    private final DataCollection dataCollection;
    private DumboQuery.IndexComparision query;

    public SearchIndexQueryMessage(DataCollection dataCollection, DumboQuery.IndexComparision query) {
        this.dataCollection = dataCollection;
        this.query = query;
    }

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public DumboQuery.IndexComparision getQuery() {
        return query;
    }
}
