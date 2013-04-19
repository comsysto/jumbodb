package org.jumbodb.connector.hadoop.index.json;

import org.jumbodb.common.query.IndexClause;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.connector.query.JumboQueryConnection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: carsten
 * Date: 12/3/12
 * Time: 7:11 PM
 */
public class TwitterQuery {
    public static void main(String[] args) throws Exception {
        JumboQueryConnection jumboDriver = new JumboQueryConnection("localhost", 12002);
//        JumboQueryConnection jumboDriver = new JumboQueryConnection("ex4s-dev01.devproof.org", 12002);
        JumboQuery query = new JumboQuery();
        query.addIndexQuery("screen_name", Arrays.asList(new IndexClause(QueryOperation.EQ, "alexjenkins29")));
//        List<Object> alexjenkins29 = new ArrayList<Object>(Arrays.asList("alexjenkins29"));
//        query.addJsonComparision(JumboQuery.JsonComparisionType.EQUALS,"user.screen_name", alexjenkins29);
//        query.addJsonComparision(JumboQuery.JsonComparisionType.EQUALS, "_id.date", Arrays.asList((Object)new Long(20121002)));
//        query.addJsonComparision(JumboQuery.JsonComparisionType.EQUALS, "_id.toCell", Arrays.asList((Object)"11211422244", "1121332341112"));
        long start = System.currentTimeMillis();
        List<Map> daily = jumboDriver.find("carsten.twitter", Map.class, query);
        System.out.println(daily);
        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
    }
}
