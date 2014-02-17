package org.jumbodb.connector.hadoop.test;

import org.codehaus.jackson.map.ObjectMapper;
import org.jumbodb.common.query.JsonQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.connector.query.JumboQueryConnection;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: carsten
 * Date: 12/3/12
 * Time: 7:11 PM
 */
public class TwitterEx10Query {
    public static void main(String[] args) throws Exception {
        JumboQueryConnection jumboDriver = new JumboQueryConnection("ex10-dev01.devproof.org", 12002);
//        JumboQueryConnection jumboDriver = new JumboQueryConnection("ex4s-dev01.devproof.org", 12002);
        JumboQuery query = new JumboQuery();
        query.setResultCacheEnabled(false);
//        query.addIndexQuery("screen_name",  Arrays.asList(new QueryClause(QueryOperation.EQ, "alexjenkins29")));
//        query.setLimit(20);

//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.EQ, "2013-04-17 22:13:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.LT, sdf.parse("2013-04-17 22:15:59").getTime())));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(sdf.parse("2013-04-17 22:13:59").getTime(), sdf.parse("2013-04-17 22:14:59").getTime()))));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.LT, "2013-04-17 22:15:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.GT, "2013-05-03 00:00:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.NE, "2013-04-17 22:13:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList("2013-05-02 22:13:00", "2013-05-02 22:15:00"))));

//        query.addJsonQuery("geo.coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(48.140, 11.562), 100000))));
//        query.addJsonQuery("geo.coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(48.140, 11.562), 100000)), new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(26.29859, 44.79119), 100000))));
// result 223
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(48.140, 11.562), 5000))));

//        JsonQuery subQuery = new JsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 10000)));
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(48.132, 11.560), 100000), Arrays.asList(subQuery))));
        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(26.29859, 44.79119), 100000))));
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(26.29859, 44.79119), Arrays.asList(26.2869, 44.8163)))));
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(26.29859, 44.79119), Arrays.asList(26.2869, 44.8163)))));

//        query.addIndexQuery("user_followers_count", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(900000, 1000000))));
//        query.addIndexQuery("user_followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 10000000)));

//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.EQ, 102100)));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.NE, 0)));
//        query.addIndexQuery("user_followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 1000)));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.LT, 10)));


//        query.addJsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 10000)));


        ObjectMapper om = new ObjectMapper();
        System.out.println(om.writeValueAsString(query));
        long start = System.currentTimeMillis();
        List<Map> daily = jumboDriver.find("twitter", Map.class, query);
        for (Map map : daily) {
            System.out.println(map.get("created_at") + " (" + ((Map) map.get("user")).get("screen_name") + ")" + " -> " + map.get("text"));
            System.out.println("==========");
        }
        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
    }
}
