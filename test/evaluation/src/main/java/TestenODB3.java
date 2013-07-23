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
public class TestenODB3 {
    public static void main(String[] args) throws Exception {
//        JumboQueryConnection jumboDriver = new JumboQueryConnection("localhost", 12002);
//        JumboQuery query = new JumboQuery();
//        query.addIndexComparision("tocellid_date", Arrays.asList("1124332342224-20121002", "1121332314344-20121002"));
//        long start = System.currentTimeMillis();
//        List<Map> daily = jumboDriver.find("de.catchment.aggregated.daily.sum.by_cell", Map.class, query);
//        System.out.println(daily);
//        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));

        JumboQueryConnection jumboDriver = new JumboQueryConnection("smartsteps-dev01.ec2.smartste.ps", 12002);
        JumboQuery query = new JumboQuery();
//        query.addIndexQuery("cellid", Arrays.asList(new QueryClause(QueryOperation.EQ, "1122331441214")));
//        query.addJsonQuery("cellid", Arrays.asList(new QueryClause(QueryOperation.EQ, "1122331441214")));
        long start = System.currentTimeMillis();
        List<Map> daily = jumboDriver.find("uk.cells", Map.class, query);
        System.out.println(daily);
        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
    }
}
