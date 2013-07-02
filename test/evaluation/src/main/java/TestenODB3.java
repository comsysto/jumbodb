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

        JumboQueryConnection jumboDriver = new JumboQueryConnection("smartsteps-jumbo-dev01.ec2.smartste.ps", 12002);
        JumboQuery query = new JumboQuery();
        query.addIndexQuery("does_not_Exist", Arrays.asList(new QueryClause(QueryOperation.EQ, "whatever")));
        long start = System.currentTimeMillis();
        List<Map> daily = jumboDriver.find("uk.catchment.aggregated.daily.sum.by_cella", Map.class, query);
        System.out.println(daily);
        System.out.println("Size " + daily.size() + " Time: " + (System.currentTimeMillis() - start));
    }
}

/*
35 datasets

[{basic=0.7325972033138191, _id={date=20121002, fromCell=11211133341, toCell=1121332314344}}, {basic=0.02708359194994763, _id={date=20121002, fromCell=1121241311412, toCell=1121332314344}}, {basic=0.5310226270424965, _id={date=20121002, fromCell=1121242323341, toCell=1121332314344}}, {basic=0.3719877265027144, _id={date=20121002, fromCell=11212432244, toCell=1121332314344}}, {basic=0.04062621973209072, _id={date=20121002, fromCell=1121311433, toCell=1121332314344}}, {basic=1.524421373675377, _id={date=20121002, fromCell=11213143124, toCell=1121332314344}}, {basic=0.06789572172720965, _id={date=20121002, fromCell=1121321112144, toCell=1121332314344}}, {basic=0.3545227087926252, _id={date=20121002, fromCell=11213212311, toCell=1121332314344}}, {basic=0.12943443328930865, _id={date=20121002, fromCell=1121321314, toCell=1121332314344}}, {basic=0.15183629912144342, _id={date=20121002, fromCell=1121321432, toCell=1121332314344}}, {basic=0.02227518716185018, _id={date=20121002, fromCell=11213221141, toCell=1121332314344}}, {basic=0.020265495910471363, _id={date=20121002, fromCell=11213223412, toCell=1121332314344}}, {basic=0.4761721416955123, _id={date=20121002, fromCell=1121322434, toCell=1121332314344}}, {basic=0.5205407323837514, _id={date=20121002, fromCell=11213231313, toCell=1121332314344}}, {basic=0.04107915016826636, _id={date=20121002, fromCell=11213233442, toCell=1121332314344}}, {basic=1.434168250100135, _id={date=20121002, fromCell=1121323412, toCell=1121332314344}}, {basic=0.17294619274817516, _id={date=20121002, fromCell=1121323423413, toCell=1121332314344}}, {basic=0.019670320520995405, _id={date=20121002, fromCell=112132421222, toCell=1121332314344}}, {basic=0.4833832860336971, _id={date=20121002, fromCell=11213242213, toCell=1121332314344}}, {basic=0.487033280159191, _id={date=20121002, fromCell=11213321324, toCell=1121332314344}}, {basic=6.675120970092381, _id={date=20121002, fromCell=112133222, toCell=1121332314344}}, {basic=41.954130153, _id={date=20121002, fromCell=1121332314344, toCell=1121332314344}}, {basic=9.257812726, _id={date=20121002, fromCell=1121332314411, toCell=1121332314344}}, {basic=4.337345378, _id={date=20121002, fromCell=11213323222, toCell=1121332314344}}, {basic=4.337345378, _id={date=20121002, fromCell=11213323222, toCell=1121332314344}}, {basic=4.647515032, _id={date=20121002, fromCell=1121332323, toCell=1121332314344}}, {basic=4.647515032, _id={date=20121002, fromCell=1121332323, toCell=1121332314344}}, {basic=4.647515032, _id={date=20121002, fromCell=1121332323, toCell=1121332314344}}, {basic=0.5615847605308055, _id={date=20121002, fromCell=11213323343, toCell=1121332314344}}, {basic=4.706827978, _id={date=20121002, fromCell=1121332341234, toCell=1121332314344}}, {basic=0.6583087444898688, _id={date=20121002, fromCell=1121332342111, toCell=1121332314344}}, {basic=3.7917829232932028, _id={date=20121002, fromCell=11213323422, toCell=1121332314344}}, {basic=4.841161491, _id={date=20121002, fromCell=11213323431, toCell=1121332314344}}, {basic=0.2558555258615361, _id={date=20121002, fromCell=1121142211, toCell=1124332342224}}, {basic=0.2069034542703851, _id={date=20121002, fromCell=11213323431, toCell=1124332342224}}]
*/