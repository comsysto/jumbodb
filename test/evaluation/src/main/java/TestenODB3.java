import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.common.query.JumboQuery;
import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.connector.query.JumboQueryConnection;

import java.util.List;
import java.util.Map;

/**
 * User: carsten
 * Date: 12/3/12
 * Time: 7:11 PM
 */
public class TestenODB3 {
    public static void main(String[] args) throws Exception {
        //        JumboQueryConnection jumboDriver = new JumboQueryConnection("localhost", 12002);
        //        JumboQuery query = new JumboQuery();
        //        query.addIndexComparision("tocellid_date", Arrays.asList("1124332342224-20121002", "1121332314344-20121002"));
        //        long start = System.currentTimeMillis();
        //        List<Map> daily = jumboDriver.find("de.catchment.aggregated.daily.sum.by_cell", Map.class, query);
        //        System.out.println(daily);
        //        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
        //        query.addIndexQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(51.542278d, -0.119877d), Arrays.asList(51.577070d, -0.027180d)))));
        //        query.addIndexQuery("screen_name", Arrays.asList(new QueryClause(QueryOperation.EQ, "AluRockyMnml")));
        //        query.addDataQuery("user.screen_name", Arrays.asList(new QueryClause(QueryOperation.EQ, "Saiya32")));
        //        query.addDataQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(50.542278d, -1.119877d), Arrays.asList(51.577070d, -0.027180d)))));
        //        query.addIndexQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(50.542278d, -1.119877d), Arrays.asList(51.577070d, 0.027180d)))));
        //        query.addDataQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(51.542278d, 0.119877d), Arrays.asList(51.577070d, 0.027180d)))));

        //        query.addIndexQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(51.542278d, -0.119877d), Arrays.asList(51.577070d, 0.027180d)))));
        //        query.addDataQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(Arrays.asList(51.542278d, -0.119877d), Arrays.asList(51.577070d, 0.027180d)))));

        //        query.addDataQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(51.577070d, 0.027180d), 30000))));
        //        query.addIndexQuery("centerLatLon", Arrays.asList(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(Arrays.asList(-51.577070d, 0.027180d), 30000))));


        JumboQueryConnection jumboDriver = new JumboQueryConnection("localhost", 12002);
        JumboQuery query = new JumboQuery();
        query.addIndexQuery(new IndexQuery("screen_name", QueryOperation.EQ, "AluRockyMnml"));
        query.setCollection("twitter_lb");
        query.setResultCacheEnabled(false);
        long start = System.currentTimeMillis();
        List<Map> daily = jumboDriver.find(Map.class, query);
//        List<Map> daily = jumboDriver.find(Map.class, "SELECT * FROM twitter_lb where user.screen_name = 'Mahahathali'");
        System.out.println(daily);
        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
    }
}
