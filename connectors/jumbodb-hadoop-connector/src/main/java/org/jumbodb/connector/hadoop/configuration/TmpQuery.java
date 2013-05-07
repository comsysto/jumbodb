package org.jumbodb.connector.hadoop.configuration;

import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryClause;
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
public class TmpQuery {
    public static void main(String[] args) throws Exception {
        JumboQueryConnection jumboDriver = new JumboQueryConnection("localhost", 12002);
//        JumboQueryConnection jumboDriver = new JumboQueryConnection("ex4s-dev01.devproof.org", 12002);
        JumboQuery query = new JumboQuery();
//        query.setLimit(10);
        query.addIndexQuery("date_cellid_sec_seg",  Arrays.asList(new QueryClause(QueryOperation.EQ, "20120921-112322241343-all-all")));
//        query.addJsonQuery("date",  Arrays.asList(new QueryClause(QueryOperation.EQ, 20120921)));
//        query.addJsonQuery("sector",  Arrays.asList(new QueryClause(QueryOperation.EQ, "foodRetail")));

//        query.addJsonQuery("date",  Arrays.asList(new QueryClause(QueryOperation.EQ, 20120805)));
//        query.addJsonQuery("sector",  Arrays.asList(new QueryClause(QueryOperation.EQ, "all")));
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.EQ, "2013-04-17 22:13:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.LT, sdf.parse("2013-04-17 22:15:59").getTime())));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(sdf.parse("2013-04-17 22:13:59").getTime(), sdf.parse("2013-04-17 22:14:59").getTime()))));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.LT, "2013-04-17 22:15:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.GT, "2013-04-17 22:15:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.NE, "2013-04-17 22:13:59")));
//        query.addIndexQuery("created_at",  Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList("2013-04-17 22:13:59", "2013-04-17 22:15:59"))));

//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(48.2025, 11.3325), 1000000))));
//        query.addJsonQuery("geo.coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(26.29859, 44.79119), 100000))));
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(26.29859, 44.79119), 100000))));
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(26.29859, 44.79119), Arrays.asList(26.2869, 44.8163)))));
//        query.addIndexQuery("coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(26.29859, 44.79119), Arrays.asList(26.2869, 44.8163)))));
//        query.addJsonQuery("geo.coordinates", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(26.29859, 44.79119), Arrays.asList(26.2869, 44.8163)))));

//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(150, 200))));
//        query.addJsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(150, 200))));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 10)));
//        query.addJsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 10)));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.EQ, 102100)));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.NE, 0)));
//        query.addJsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.NE, 0)));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 100000)));
//        query.addJsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.GT, 100000)));
//        query.addIndexQuery("followers_count", Arrays.asList(new QueryClause(QueryOperation.LT, 10)));
//        query.addJsonQuery("user.followers_count", Arrays.asList(new QueryClause(QueryOperation.LT, 10)));


//        query.addIndexQuery("followers_count_long", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(150, 200))));
//        query.addIndexQuery("followers_count_long", Arrays.asList(new QueryClause(QueryOperation.EQ, 102100)));
//        query.addIndexQuery("followers_count_long", Arrays.asList(new QueryClause(QueryOperation.NE, 0)));
//        query.addIndexQuery("followers_count_long", Arrays.asList(new QueryClause(QueryOperation.GT, 100000)));
//        query.addIndexQuery("followers_count_long", Arrays.asList(new QueryClause(QueryOperation.LT, 10)));

//        query.addIndexQuery("followers_count_double", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(150, 200))));
//        query.addIndexQuery("followers_count_double", Arrays.asList(new QueryClause(QueryOperation.EQ, 102100)));
//        query.addIndexQuery("followers_count_double", Arrays.asList(new QueryClause(QueryOperation.NE, 0)));
//        query.addIndexQuery("followers_count_double", Arrays.asList(new QueryClause(QueryOperation.GT, 100000)));
//        query.addIndexQuery("followers_count_double", Arrays.asList(new QueryClause(QueryOperation.LT, 10)));

//            query.addIndexQuery("followers_count_float", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, Arrays.asList(150, 200))));
//        query.addIndexQuery("followers_count_float", Arrays.asList(new QueryClause(QueryOperation.EQ, 102100)));
//        query.addIndexQuery("followers_count_float", Arrays.asList(new QueryClause(QueryOperation.NE, 0)));
//        query.addIndexQuery("followers_count_float", Arrays.asList(new QueryClause(QueryOperation.GT, 100000)));
//        query.addIndexQuery("followers_count_float", Arrays.asList(new QueryClause(QueryOperation.LT, 10)));

//        List<Object> alexjenkins29 = new ArrayList<Object>(Arrays.asList("alexjenkins29"));
//        query.addJsonQuery(JumboQuery.JsonComparisionType.EQUALS,"user.screen_name", alexjenkins29);
//        query.addJsonQuery(JumboQuery.JsonComparisionType.EQUALS, "_id.date", Arrays.asList((Object)new Long(20121002)));
//        query.addJsonQuery(JumboQuery.JsonComparisionType.EQUALS, "_id.toCell", Arrays.asList((Object)"11211422244", "1121332341112"));
        long start = System.currentTimeMillis();
        List<Map> daily = jumboDriver.find("uk.visa.aggregated.daily.sum.by_cell", Map.class, query);
        for (Map map : daily) {
            System.out.println(map);
        }
        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
    }
}
