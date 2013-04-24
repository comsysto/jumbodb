package org.jumbodb.common.query;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Carsten Hufe
 */
public class TestenQuery {

    public static void main(String[] args) throws IOException {
        JumboQuery j = new JumboQuery();
//        j.addIndexQuery("index", Arrays.asList(new QueryClause(QueryOperation.EQ, Arrays.asList("Hallo", "Hallo2"))));
        j.addIndexQuery("index", Arrays.asList(new QueryClause(QueryOperation.BETWEEN, new BetweenQuery(123, 59479))));

        ObjectMapper om = new ObjectMapper();
        String str = om.writeValueAsString(j);
        System.out.println(str);
        System.out.println(om.readValue(str, JumboQuery.class));
    }
}
